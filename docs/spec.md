# bloar: Specification (v1)

This is the normative spec for implementing bloar. Rationale lives in
[design.md](design.md). Requirement words (MUST, SHOULD, MAY) are used in the
RFC 2119 sense.

## 1. Terminology and constants

| Term | Meaning |
|---|---|
| blob | An EIP-4844 blob: exactly `BLOB_SIZE` bytes. |
| vh | Versioned hash: `0x01 || sha256(kzg_commitment)[1:]`, 32 bytes. |
| slot | Beacon chain slot number (uint64). |
| head | A named, filtered index of `(slot, vh) -> blob CID`. |
| segment | All refs for one aligned window of `2^seg_bits` slots. |
| directory | Implicit radix tree of sealed segment CIDs. |
| ordinal | `slot >> seg_bits`: which segment window a slot belongs to. |
| row | `(slot, [vh...])`: the refs for one blob-carrying slot. |

Constants:

```
BLOB_SIZE            = 131072            # bytes, fixed by EIP-4844
MAINNET_GENESIS_TIME = 1606824023
SECONDS_PER_SLOT     = 12                # from beacon spec, configurable per network
DENCUN_MAINNET_SLOT  = 8626176           # first possible blob slot on mainnet
```

`origin_slot` per deployment/head defaults to the network's first blob slot.

## 2. Hashing, CIDs, and encoding

- **Blob blocks**: CIDv1, codec `raw` (0x55), multihash `sha2-256`. The block
  payload is the exact `BLOB_SIZE` blob bytes. One block per blob, never
  chunked.
- **Index blocks** (Head, DirNode, Segment, Manifest): CIDv1, codec `dag-cbor`
  (0x71), multihash `sha2-256`. A Manifest (10.5) is a chain block, not part of a
  head's Head -> directory -> segments DAG; it is encoded and addressed by the
  same rules as the rest.
- All DAG-CBOR MUST be encoded canonically (DAG-CBOR strict rules: definite
  lengths, map keys sorted, no floats, links as tag 42). Implementations MUST
  produce byte-identical encodings for identical logical content; CID
  stability across implementations is a conformance requirement (see 13).
- Links between index blocks and from segments to blobs MUST be real IPLD
  links (tag 42), never bare bytes, so that recursive pinning traverses the
  full DAG.
- `vh` values are 32-byte CBOR byte strings, not links (they are not CIDs). A
  Manifest's source `address`, `topic`, and `sender` fields (10.5) are likewise
  CBOR byte strings (20 or 32 bytes), not links; only a Manifest's `prev` is a
  link.

## 3. Object schemas

Field names are the literal DAG-CBOR map keys.

### 3.1 Head

The root object of one head. A new Head block is written on every update; the
latest CID is published out-of-band (section 8).

```
Head = {
  "v":           1,              # schema version, uint
  "name":        string,         # head name, e.g. "all", "arbitrum-one"
  "net":         string,         # e.g. "mainnet", "sepolia"
  "origin_slot": uint,           # first slot this head covers (inclusive)
  "synced_to":   uint | null,    # last covered slot (inclusive); null = empty
  "seg_bits":    uint,           # segment coverage = 2^seg_bits slots
  "fanout_bits": uint,           # directory fanout = 2^fanout_bits
  "dir_depth":   uint,           # directory tree depth (0 = no directory yet)
  "dir":         link | null,    # DirNode root (null iff dir_depth == 0)
  "open":        link | null,    # open Segment (null if empty)
}
```

`name` MUST match `[a-z0-9][a-z0-9-]*`. `seg_bits`, `fanout_bits`, and
`origin_slot` are immutable for the life of a head; changing them means
building a new head (a rebuild reuses all blob blocks).

### 3.2 Segment

All refs for the window `[ord << seg_bits, (ord+1) << seg_bits)`.

```
Segment = {
  "v":     1,
  "slot0": uint,                 # window start = ord << seg_bits
  "rows":  [ Row, ... ],         # ascending by slot, no duplicate slots
}

Row = [ slot(uint), [ RefEntry, ... ] ]     # >= 1 entry
RefEntry = [ vh(bytes32), blob(link) ]
```

- `rows` contains only blob-carrying slots. Blobless / missed slots are
  absent.
- Within a row, entries preserve the blob order of the beacon block for the
  ALL head; for chain heads, encounter order (L1 tx index, then in-tx order),
  deduplicated. Order is part of the content (affects the CID) and MUST be
  deterministic.
- A fully-empty window seals to **no object at all** (null directory entry).

### 3.3 DirNode

```
DirNode = {
  "v":    1,
  "kids": [ link | null, ... ],  # length <= 2^fanout_bits
}
```

At depth 1, `kids` link to Segments; at depth > 1, to child DirNodes.
Trailing nulls MAY be omitted (readers treat out-of-range as null). Interior
nulls MUST be explicit.

The **Manifest** and **Source** DAG objects are defined in 10.5, alongside the
sources model that gives their fields meaning; they follow the same
canonical-encoding rules as the schemas above.

## 4. Arithmetic

For a head with parameters `seg_bits = k`, `fanout_bits = f`, `origin_slot`:

```
ord(slot)      = slot >> k                      # global segment ordinal
dir_base       = origin_slot >> k
idx(slot)      = ord(slot) - dir_base           # directory index, 0-based
capacity(d)    = (2^f)^d                        # max sealed segments at depth d
```

Directory lookup of sealed segment `i = idx(slot)` at depth `d`: the path
digits are `i` written base-`2^f`, most significant first, padded to `d`
digits. Walk from the root taking `kids[digit]` at each level; null anywhere
means "no refs in that range".

Serving lookup for `(slot, vh)`:

```
if slot < head.origin_slot:                          BEFORE_ORIGIN
if head.synced_to == null or slot > head.synced_to: NOT_YET_COVERED
if ord(slot) == ord(head.open window):               search head.open rows
else:                                                walk directory to segment,
                                                     search rows (binary search)
row found: match vhs; absent row or absent vh:       DEFINITELY_ABSENT
```

BEFORE_ORIGIN is checked first: a slot the head is defined never to cover
must map to 404, not to 503 + Retry-After (which a client would retry
forever), even while the head is empty.

## 5. Head mutation algorithms

All mutation is single-writer per head (the `bloard` daemon serializes it).
Mutation is copy-on-write: build new blocks bottom-up, then atomically swap
the published root. A crash mid-update leaves orphan blocks (collected by GC)
and the old root intact.

### 5.1 apply_refs(head, rows, new_synced_to)

Input validation (reject the whole batch with 409 on any failure, except the
idempotent-replay case):

1. `rows` sorted strictly ascending by slot; every row slot in
   `[origin_slot, new_synced_to]`; `new_synced_to >= last row slot` (rows MAY
   be empty if only advancing coverage).
2. **Idempotent replay**: if every row slot `<= synced_to` and
   `new_synced_to <= synced_to`: verify each row exactly matches the stored
   row (same vhs, same order). Match -> no-op success. Mismatch -> 409.
3. Otherwise every row slot MUST be `> synced_to` (no partial overlap).
4. Every referenced vh MUST resolve in the blob catalog (section 6.1) AND the
   block MUST exist in the blockstore. Failure -> 409 listing missing vhs.

Apply:

```
for w in [ord(synced_to+1 or origin_slot) .. ord(new_synced_to)]:
    add rows with ord(row.slot) == w to the open segment (extending it)
    if w fully covered by new_synced_to (i.e. (w+1)<<k - 1 <= new_synced_to):
        seal(w)
head.synced_to = new_synced_to
write new open Segment (if dirty), new Head; swap root; update pins; publish
```

### 5.2 seal(w)

```
if open segment has rows: store it -> cid else cid = null
dir_append(idx = w - dir_base, cid)
open = empty segment with slot0 = (w+1) << k
```

### 5.3 dir_append(i, cid)

```
if dir_depth == 0: dir = DirNode{kids:[cid]}; dir_depth = 1; return   # i == 0
if i == capacity(dir_depth):                 # root full: grow a level
    dir = DirNode{kids:[dir]}; dir_depth += 1
rewrite the spine: for each level from root to leaf along digits of i,
copy the DirNode, set/extend kids[digit] (append null-padding as needed),
link the copied child. O(dir_depth) new blocks.
```

Appends always target index `i` = current sealed count; anything else is an
internal error.

### 5.4 truncate(head, slot)  [admin, emergency only]

Restores the head to coverage `[origin_slot, slot]`:

```
require slot >= origin_slot  (or "empty" to reset the head)
t = ord(slot)
new open = rows of sealed segment t (if any) filtered to <= slot,
           else current open filtered to <= slot if ord unchanged
directory: drop all entries with idx > t - dir_base, and entry t itself
           (its remainder became the open segment); rebuild spine; shrink
           depth while the root has a single non-null child at kids[0]
if slot == (t+1)<<k - 1:  window t is fully covered: seal the rebuilt
           open segment per 5.1/5.2 (append to directory, open fresh
           empty segment over window t+1)
synced_to = slot; write, swap, re-pin, publish
```

Truncation MUST be refused if `slot > synced_to`.

For a head whose retention policy is `window`, truncation MUST close the online
GC publication gap before swapping the new root. It MUST validating-`Get`
through the application blockstore (a) every raw reference which survives in
the rebuilt target Segment and (b) every raw descendant of a sealed Segment
which the backwards-moving trailing window makes newly recursive. These checks
both reject corrupt/missing retained data and add the multihashes to the active
epoch's T set. A `full` head already had those descendants in the old root's T0
closure M; a `none` head deliberately does not retain them. The daemon MUST pass
the configured window width to the truncation engine; this is not inferred from
the DAG.

The seal step on a window-final slot is required, not optional: without
it the open segment would be a fully-covered window, violating the
invariant that the open segment is always window `ord(synced_to + 1)`;
the next apply would resume at window t+1 and window t's rows would
never enter the directory, diverging from a fresh build of the same
data.

Truncation is also the first step of the manifest-chain recovery order (10.5).
Because it moves `synced_to` -- a head's position -- back, it is what lets a
corrected manifest legally change rules the old position had already covered.
The mechanically enforced sequence is truncate, then publish the corrected
manifest, then resync; manifest-first is rejected by the indexer's append-only
check (10.5), so truncate-first is not a convention but the only order that
passes. The deep-truncate case -- coverage dropping below the immutable horizon
(7.1), where long-lived cache entries must be purged -- is operational guidance
in the runbook ([operations.md](operations.md)).

## 6. Node-local state (not part of the DAG)

Blocks (blobs and index nodes) live in a boxo blockstore backed by flatfs
(`go-ds-flatfs`): one file per block, shard function
`/repo/flatfs/shard/v1/next-to-last/3`, sync writes enabled. The shard
function is fixed at store creation.

Everything else below is kept in a Pebble (`cockroachdb/pebble`) KV store
next to the blockstore. Not all of it is rebuildable from the DAG. The blob
catalog (6.1) and the reconciled part of the pin ledger (6.2) are derived
caches, re-derivable by re-verification; a `bloard rebuild` subcommand MUST
exist and restores exactly the catalog. The rest is not derivable from an
unordered blockstore and MUST be classified as such:

- **Current-selection state.** Head roots and manifest tips are the CIDs a node
  is currently serving; nothing in a store full of Head or Manifest blocks says
  which one is current. On a writer these are authoritative restart selectors
  with no import path; on a follower they are write-through mirrors of the
  follower checkpoint (11.3).
- **Monotonic publication state.** The writer's IPNS sequence (8.1) is a
  non-derivable counter, not a selector or an identity: losing it restarts the
  sequence and a resolver may retain a higher external record.
- **Anti-replay floors.** A follower's checkpoint generation and its retained
  freshness floors (11.3) are exactly what a next poll CANNOT re-establish --
  their whole purpose is to remember that the last accepted document was at least
  this fresh.
- **Leases.** Staging pins (section 9) carry a TTL: time-bearing state a rebuild
  cannot reconstruct.
- **Derived semantic-verification cache.** A follower in `verify: full` stores a
  versioned marker keyed by each successfully checked sealed Segment CID. The
  marker is an optimization, never authority: absence or loss causes the Segment
  to be reverified. Open-Segment proofs remain memory-only (11.4).

Operations documents the backup and recovery consequences of each class.

### 6.1 Blob catalog

`vh (32 bytes) -> blob CID`. Written by `put blobs` after verification. Read
by `apply_refs`. Entries MAY outlive their blocks (GC does not update the
catalog), which is why `apply_refs` also checks blockstore presence.

### 6.2 Pin ledger

The set of pins the daemon believes it holds, per head and purpose, so pin
reconciliation (section 9) is diffable and crash-safe: compute desired set,
add missing pins, update ledger, remove stale pins, in that order.

Ledger values are `flags[1] [|| expiry[8]]` — bit 0 recursive, bit 1
expires, expiry Unix seconds big-endian; legacy 1-byte rows decode as
no-expiry. The reserved head name `_staging` files ingest's staging pins
(section 9) and cannot collide with a real head: 3.1's name grammar admits
no underscore. Reconciliation never touches the reserved head.

### 6.3 Decoded-node cache

In-memory LRU of decoded index nodes, read-through behind the node store
(pointer loads populate from it; dirty values never enter it). Purely a
performance feature; correctness MUST NOT depend on it. Sized by
`store.node_cache_mb`, approximating per-entry cost by encoded block
length — a floor on memory held, not a ceiling.

## 7. HTTP API

All JSON. Errors use beacon-API shape: `{"code": <int>, "message": "<str>"}`.

### 7.1 Beacon-compatible read API (public, per head)

Mounted at `/{head}/eth/v1/...`. Unknown `{head}` -> 404.

#### GET /{head}/eth/v1/beacon/blobs/{slot}?versioned_hashes=0x..&versioned_hashes=0x..

- `{slot}` MUST be a decimal slot number (named block ids not supported).
- `versioned_hashes` is an order-significant array. The canonical Beacon
  OpenAPI encoding repeats the query key; a comma-separated value is also
  accepted for Base compatibility, and the two forms may be mixed. A request
  MUST name at most `server.max_query_hashes` decoded entries, **duplicates
  counted** (default 128, a ceiling above any fork's blobs per slot); a request
  naming more is `400` before any lookup. Duplicates at or below the bound
  remain legal and are answered with their multiplicity and order preserved.
  The endpoint reads one blob per entry and buffers the whole response before
  writing it, so the expanded count bound (together with the per-slot
  stored-row ceiling below) is what bounds a single response; a
  process-wide response-memory budget (`server.max_response_bytes_in_flight`)
  bounds all in-flight responses at once. Together these bounds prevent
  duplicate-query amplification from exhausting response memory.
- Responses:
  - `200` `{"data": ["0x<131072-byte hex>", ...]}`
    - With filter: exactly one blob per requested vh, **in request order**.
    - Without filter: all blobs at the slot in stored (canonical) order;
      `{"data": []}` if the covered slot has none. (Divergence from a real
      beacon node, which 404s on missed slots; the archive does not track
      block presence. Nitro never queries blobless slots.) A stored slot
      carries at most the per-slot blob ceiling (128), enforced when refs are
      applied, so an unfiltered response is bounded by construction.
  - `400` if the request names more than `server.max_query_hashes`
    versioned_hashes after expanding either array encoding (duplicates
    counted), or a versioned hash is malformed.
  - `404` if `slot < origin_slot`, or if any requested vh is definitively
    absent at a covered slot (message names the first missing vh).
  - `503` + `Retry-After: 12` if `slot > synced_to` (not yet archived).
  - On a follower, a covered slot whose blob block is not locally present
    triggers a bounded on-demand fetch (11.4); on fetch failure, `503` +
    `Retry-After: 12`, `Cache-Control: no-store`.
- Response encoding: JSON (above) is the default. A client that sends `Accept:
  application/octet-stream` receives, on a `200`, the same blobs in the same
  order this list would state them -- request order under a `versioned_hashes`
  filter, stored (canonical) order otherwise -- as the raw concatenation of N
  blobs, each exactly `BLOB_SIZE` bytes (the blob-body convention of 7.2), under
  `Content-Type: application/octet-stream`; the fixed size self-frames them, and
  a covered slot carrying nothing is a `200` with an empty body. This is the
  only variation. Any other `Accept` -- absent, `application/json`, `*/*`, or
  unrecognised -- gets the JSON body above byte-for-byte, and `400`/`404`/`503`
  stay JSON regardless of `Accept`. Caching headers are identical either way.
- Caching headers:
  - covered `200`/`404`: `Cache-Control: public, max-age=31536000, immutable`
    for slots older than the configurable immutable horizon (default 1 day
    behind `synced_to`); `public, max-age=60` for newer covered slots
    (truncate is theoretically possible).
  - `503`: `Cache-Control: no-store`.

When a local virtual head is configured in `live_heads`, its beacon blobs path
joins one `finalized-monotonic` physical head and one `unfinalized-mutable`
physical head without creating a third publication entry. One atomic local
registry snapshot selects the answer. A version-2 or version-3 mutable entry
names its finalized handoff and carries the exact handoff root/frontier from the
same authenticated document.

In the ordinary mode, the virtual finalized name MUST equal that authenticated
handoff name. A locally written mutable head and a follower which selects both
physical inputs therefore expose one exact coherent pair; a cross-wired view
fails startup validation. An exact-hash overlay MAY instead set
`require_versioned_hashes: true` and pair the global mutable tip with a different
chain-filtered finalized head. The writer's global handoff line remains part of
the signed proof, but a filtered follower treats it as metadata only: it MUST NOT
select, fetch, independently checkpoint, pin, serve, or republish that line. Its
exact contents MAY be nested in the mutable checkpoint so restart can reconstruct
the proof without creating a selected global head. The follower MUST select the
filtered finalized line and mutable line from the same authenticated document,
and MUST refuse the whole document before any adoption when the
filtered line is absent/uncovered or `mutable.window_start >
filtered.synced_to + 1`. Such a refusal is counted as `handoff_blocked`. The two
selected checkpoints, mirrors, serving pointers, pin delta, withdrawal state,
and replay floor change atomically; external-store retirement also waits for
readers of the retired snapshot. A concurrent GC can therefore observe the old
pair or the new pair, never an adopted gap between them.

A mixed-role node MAY write the filtered finalized head locally while following
only the global mutable head. The local frontier is not a claim in the remote
document, so document admission cannot bind the pair. Each physical transition
still shares the registry/GC gate, and the live selector applies the same gap
test to one immutable registry snapshot; an uncovered interval is a retryable
`503`, not a false absence. `handoff_blocked` applies to the all-followed case.

At or below the selected finalized head's durable `synced_to`, that head is
authoritative for both presence and absence; a finalized 404 MUST NOT fall back
to the mutable head. Above that frontier, the mutable head is selected only when
its durable complete generation covers the requested slot. Ordinary views may
enumerate that provisional slot. An exact-hash overlay MUST receive at least one
`versioned_hashes` value for a provisional request; an unfiltered provisional
request is `400` + `Cache-Control: no-store`. The hashes are lookup keys, not an
authorization list: any exact hash present in the authenticated global mutable
tip remains requestable, including one from another chain. Finalized requests
remain enumerable. A missing startup generation, quarantine, or gap returns
`503` with `Retry-After: 12` and `Cache-Control: no-store` rather than guessing.
Responses selected from the finalized head carry `X-Bloar-Finality: finalized`
and retain its normal cache policy. Responses selected from the mutable head
carry `X-Bloar-Finality: provisional`; every provisional 200/404/503 is
`no-store`. The bounded mutable head is retained in full. Blobs not referenced
by the filtered finalized archive rotate out with old mutable generations;
blocks shared with the filtered archive remain reachable there.

The virtual name is local serving policy: it is absent from `/bloar/v1/heads`
and is not a mutation target. Its `/genesis` and `/config/spec` endpoints use the
finalized physical head as their availability authority and carry the finalized
marker.

#### GET /{head}/eth/v1/beacon/genesis

`200 {"data": {"genesis_time": "<uint string>", "genesis_validators_root":
"0x...", "genesis_fork_version": "0x..."}}` -- static values from network
config. Nitro reads `genesis_time`.

#### GET /{head}/eth/v1/config/spec

`200 {"data": {"SECONDS_PER_SLOT": "12", ...}}` -- static map from config;
MUST include `SECONDS_PER_SLOT`. Additional keys passthrough from config.

All public GET routes in 7.1 and the public reads in 7.2 pass through default-on
weighted admission before lookup or allocation. Metadata costs 1 unit. A
filtered blobs request costs `1 + len(versioned_hashes)` after repeated-key or
comma-separated array values are expanded, duplicates included; an unfiltered
or syntactically over-cap request costs the conservative maximum,
`1 + server.max_query_hashes`. Admission is an atomic, non-waiting reservation
from a process-wide token bucket and a per-client token bucket. Rejection
consumes neither bucket and returns `429`, an integer `Retry-After`, and
`Cache-Control: no-store`.

The socket peer is the client by default, with IPv6 aggregated to /64. A
configured IP-list forwarding header is honored only when the socket peer is in
an explicitly trusted canonical CIDR; the rightmost untrusted address is used.
Client bucket state is TTL/LRU bounded and the global bucket remains
authoritative across churn and eviction. Authenticated mutations and the
private operational listener are outside this admission layer.

### 7.2 bloar API

#### GET /bloar/v1/heads  (public)

The publication document (section 8). `Cache-Control: public, max-age=12`.

#### GET /bloar/v1/heads/{head}  (public)

Single head entry from the same document, plus `origin_slot` etc. 404 if
unknown.

#### POST /bloar/v1/blobs  (auth)

- Body: `application/octet-stream`, concatenation of N blobs. `N =
  len(body) / BLOB_SIZE`; reject 400 if not divisible or `N >
  max_put_blobs` (config, default 64; advertised on `GET /bloar/v1/heads` so an
  indexer cross-checks the durable local `archive.max_put_blobs` expectation
  whenever its finalized loop is constructed, section 8). The local expectation
  remains the safety bound while the archive is unavailable; a differing
  advertised value is configuration drift and fails closed before any write.
- For each blob: compute KZG commitment (reject 400 with the offending index
  if the bytes are not canonical field elements), derive vh, compute CID,
  store block (idempotent), upsert catalog.
- `200 {"blobs": [{"versioned_hash": "0x..", "cid": "bafk.."}, ...]}` in body
  order. No metadata is accepted: the server derives everything.

#### POST /bloar/v1/heads/{head}/refs  (auth)

```
{"rows": [{"slot": 123, "versioned_hashes": ["0x..", ...]}, ...],
 "synced_to": 456, "expected_manifest": "bafy.."}
```

Semantics per 5.1. `expected_manifest` binds the batch to the manifest tip it was
scanned under (10.5): the server compares it to the head's
registered tip atomically with the commit, under the head lock. It is REQUIRED for
a head that has a manifest chain and FORBIDDEN for a chainless head (the ALL head,
and any head predating the manifest chain). Responses: `200 {"synced_to": n,
"root": "bafy..", "noop": bool}`; `409` with `{"code":409, "message":...,
"missing_blobs": ["0x..",...]?, "manifest_tip": "bafy.."?}` on any validation
failure — `manifest_tip` is the head's current tip when the conflict is a stale
`expected_manifest`, and the writer stops and resyncs against it; `400` malformed,
including an `expected_manifest` whose presence does not match whether the head has
a chain.

#### GET /bloar/v1/heads/{head}/synced_to  (public)

`200 {"synced_to": n | null}`.

#### POST /bloar/v1/heads/{head}/truncate  (auth, admin)

`{"slot": n}` -> semantics per 5.4. `200 {"synced_to": n, "root": "bafy.."}`.
SHOULD require a separate confirmation field `{"confirm": "<head name>"}`.

#### POST /bloar/v1/heads/{head}/manifest  (auth, admin)

Advances the head's manifest chain (10.5), the append-only record of its filter.
This is the publish half of the append-only workflow; the semantic check runs as
the chain indexer's preflight before it (10.5, `bloar-index publish-manifest`).
Body:

```
{"manifest": {"v": 1, "head": "<head>", "sources": [ ... ], "prev": "bafy.." | null},
 "confirm": "<head name>", "expected_head_root": "bafy.."}
```

The server canonicalizes `manifest` to a dag-cbor block, computes its CID, and
commits it as the head's new tip iff, atomically under the head lock, the
compare-and-swap on `prev` holds AND `expected_head_root` equals the head's
current root. `prev` MUST equal the head's current published tip CID, or be null
when the head has no tip yet (the genesis bootstrap of 10.5). `expected_head_root`
is REQUIRED — it is the head root the preflight validated against, and the server
rejects a POST whose root has since advanced so the preflight's verdict cannot be
applied to a position it never saw (10.5).
On success it stores the block, takes the `manifest` pin (9), and republishes the
head entry with the new tip in its `manifest` field (8). Responses: `200
{"manifest": "bafy.."}`; `409` when `prev` does not match the current tip OR
`expected_head_root` does not match the current root — the writer raced another
upgrade or a refs commit, and re-runs the preflight against the current head;
`400` malformed (undecodable, a missing or bad `prev`/`expected_head_root` CID, or
a `blob-txs` source with an empty `senders`). The server does NOT check that the
new schedule is an append-only successor — it compares CIDs, not L1 (10.5); that
check is the indexer's preflight.

Like truncate, this SHOULD require a separate confirmation field `{"confirm":
"<head name>"}`. The compare-and-swap and the confirm guard different failures and
neither subsumes the other: the CAS on `prev` defends against a race — two
upgrades built from the same tip — while `confirm` defends against aim, a
rarely-run write that redefines what the head means being pointed at the wrong
head. A race the CAS catches and a fat-finger it would happily commit are not the
same failure, so both gates stand.

A public `GET /bloar/v1/heads/{head}/manifest` returns the head's current tip
-- the decoded manifest and its CID -- and `404`s a head with no chain. It is
the read channel for an indexer checking its schedule against the published tip
(10.5) and for an auditor starting a re-derivation (11.5), neither of which
holds the block over p2p; like every read here, it is public.

### 7.3 Authentication

Ingest/admin endpoints require `Authorization` matching a configured bearer
token (constant-time compare). Read endpoints are public. TLS termination is
out of scope (reverse proxy).

### 7.4 Operational endpoints

When `server.metrics_listen` is configured, a separate listener serves
`/metrics` (Prometheus), `/healthz` (liveness: replies unconditionally — a
liveness probe that consulted the store would crash-loop a daemon on a slow
disk while it holds the KV lock), and `/readyz` (readiness). `/readyz` is 200
only when every gate is met, and 503 naming the unmet gates otherwise. The
gates are: `store` (the store is open), `heads` (every configured WRITER head is
registered), `reconcile` (the first pin reconciliation has completed), `gc` (the
periodic GC scheduler is launched and running — withdrawn only if the scheduler
stops with a terminal error, NOT lowered by an individual collection failing),
and one separate `followed_head:<name>` gate per configured FOLLOWED head. A
followed-head gate means exactly: this process currently has that configured
head registered and serviceable, having adopted it from a verified publication
document — it is resumed from the durable checkpoint or first adopted from a
verified document. It is NOT a statement about lazy-DAG (verify: full) blob
verification, freshness, writer reachability, or last-poll success: an ordinary
poll failure does **not** lower it (a head that has served keeps serving its
durable generation while the writer is unreachable). A quarantine (§11.4) DOES
lower it, and within this process no later poll or resume can resurrect a
quarantined head (a restart re-evaluates from the durable checkpoint — quarantine
is process-lifetime state). So `store`/`heads`/`reconcile` are established once,
`gc` only regresses on a terminal scheduler failure, and a followed-head gate is
the one that regresses in normal operation — deliberately, so the load balancer
stops routing reads a quarantined head can only 503. Default: disabled. Metric
and gate labels are config-bounded and low-cardinality by rule: never slot, CID,
vh, or peer; unknown head names fold to one series.

## 8. Head publication

`GET /bloar/v1/heads` returns:

```
{
  "v": 3,
  "net": "mainnet",
  "archive_id": "<64 lowercase hex characters>",  # v3 only; signed logical
                                                    # archive identity
  "updated_at": "<RFC3339>",
  "multiaddrs": ["/dns4/../tcp/4001/p2p/<peerid>", ...],   # optional: where
                                                           # to bitswap from
  "heads": [
    {"name": "all", "root": "bafy..", "origin_slot": 8626176,
     "synced_to": 12345678, "seg_bits": 9, "fanout_bits": 8, "dir_depth": 2},
    {"name": "arbitrum-one", "root": "bafy..", "origin_slot": 8626176,
     "synced_to": 12345678, "seg_bits": 13, "fanout_bits": 8, "dir_depth": 2,
     "manifest": "bafy.."},                                # present iff 10.5
    {"name": "unfinalized", "root": "bafy..", "origin_slot": 12345570,
     "synced_to": 12345634, "seg_bits": 5, "fanout_bits": 8,
     "dir_depth": 1, "kind": "unfinalized-mutable",
     "window_start": 12345570,
     "source_head_root": "0x..", "source_finalized_slot": 12345600,
     "source_finalized_root": "0x..", "handoff_head": "all",
     "handoff_root": "bafy..", "handoff_synced_to": 12345600},
    ...
  ],
  "revision": 42,                       # signed; required in v3 and once
                                        # mutable heads activate;
                                        # signer-local and >= 1
  "pubkey": "<hex ed25519>",          # required in v3
  "signature": "<hex ed25519 sig>",   # required in v3; over canonical JSON of
                                      # {v, net, archive_id, updated_at,
                                      #  multiaddrs, heads, revision}
  "max_put_blobs": 64                 # the archive's POST /bloar/v1/blobs count
}                                     # limit (7.2); OUTSIDE the signature
```

If a signing key is configured, followers MUST verify. The document MUST be
updated atomically with root swaps (a reader never sees a root that was
never current). Version 1 is the legacy finalized-only schema. Version 2
introduces the proof-aware schema; every mutable entry requires version 2 or 3.
Version 3 retains that proof contract and adds the signed `archive_id`: a stable
logical archive identity which independent writer keys can share. A version-3
document MUST carry a nonzero `archive_id`, a revision, a public key, and a
valid signature. The wire spelling is exactly 64 lowercase hexadecimal
characters (32 bytes); all zeroes are reserved and invalid. Versions 1 and 2
MUST omit the field, preserving their existing canonical bytes. A version-1
mutable entry or a version-1 entry carrying version-2 proof fields is invalid.

Writer root changes are copy-on-write. The complete candidate engine and its
prospective signed publication are built before the durable root selector or
reconciler target changes. After the selector persists, reconciler retarget,
registry swap, publication swap, and notification are infallible. A publication
allocation/signing failure therefore leaves the old root, GC retention target,
registry, and document coherent; an allocated-but-unused revision is a permitted
gap.

`max_put_blobs` echoes the archive's own POST /bloar/v1/blobs count limit (7.2)
so an indexer reads it once at startup and refuses to run when its configured
`index.max_put_blobs` exceeds it — otherwise every full put would 400 minutes
into the run (10.1). It is deliberately outside the signed portion: the
signature exists for untrusting followers, and no follower acts on this field —
only an indexer does, and an indexer already holds the archive's write token. It
is omitted when zero, which is how a reader tells an archive predating the field
from one advertising a limit (none advertise zero; the server floors it to 64).

**Canonical JSON** (the signed bytes): the exact `encoding/json` marshal of
the unsigned document `{v, net, archive_id, updated_at, multiaddrs, heads,
revision}` — field order as listed, default HTML escaping, no indentation,
`heads` sorted by name, `multiaddrs` omitted entirely when empty, `synced_to` an
explicit null on an empty head. `archive_id` is present exactly in version 3;
omitting it from versions 1 and 2 preserves their canonical bytes and signature
compatibility. The served document is the unsigned document with `pubkey` and
`signature` (hex) appended. Verifiers MUST unmarshal the served document,
re-marshal its unsigned portion with the same rules, and verify over those bytes
— never re-serialize from their own struct shapes or a generic map, whose field
order and escaping would differ.

`revision` is an optional, monotonically increasing uint64 allocated by the
document signing authority. Its authority is the verified 32-byte document
signing public key — not the HTTPS URL, IPNS name or IPNS key. Revision 0 is
invalid. Once an authority has published a revisioned document, followers MUST
reject a later revisionless document from that authority as a downgrade. A lower
revision is replay; the same revision with the same SHA-256 digest of the
canonical unsigned claim is an idempotent repeat; the same revision with a
different digest is equivocation and MUST quarantine the authority's mutable
heads. A higher revision is only a candidate: validation, retained-closure
preparation and the atomic checkpoint commit happen before its floor advances.
Gaps are permitted, so a writer MAY durably allocate a revision which fails to
publish. Overflow fails closed.

For revisioned documents, `updated_at` is diagnostic only and MUST NOT order
candidates. HTTPS and IPNS candidates from one authority are ordered by
revision, with equal-revision digest disagreement detected before selection.
Revisions from different signing authorities are incomparable. Version 3 does
not change that rule: `archive_id` identifies the logical archive, but neither
orders writers nor authorizes their keys. Finalized claims from independently
authorized version-3 writers use the content partial order in section 11.3;
their revisions and clocks are never compared. During an unpinned DNSLink signer
rotation, the document authenticated through the currently selected
DNSLink/IPNS chain is the explicit authority handoff and supersedes an HTTPS
document authenticated by the previously remembered signer; without that
handoff, followers MUST refuse cross-authority transport candidates rather than
compare their revisions or clocks.
IPNS sequence remains an independent, per-name transport replay floor; it is
never a publication revision and a higher IPNS sequence carrying a lower
publication revision remains replay. Revisionless documents retain the legacy
`updated_at` ordering and floors exactly.

Every head has an authenticated ordering contract. Omitted `kind` means
`finalized-monotonic`, preserving the exact bytes of legacy entries. An explicit
`finalized-monotonic` is permitted only in a revisioned document. Its
`origin_slot` is immutable and its `synced_to` follower floor never regresses.
`unfinalized-mutable` instead represents one complete bounded snapshot:

- the document MUST be signed and revisioned;
- `window_start` and `synced_to` MUST be present, `window_start <= synced_to`,
  and `window_start` MUST equal the root's `origin_slot`;
- coverage is exactly `[window_start, synced_to]`; a missing row inside the
  range is an authenticated missed/blobless slot, while a slot outside it is
  uncovered;
- a higher document revision replaces the complete snapshot. Rows may be
  replaced or disappear, and the bounds may regress; no tombstones or deltas
  are inferred;
- it MUST NOT carry a finalized filter manifest, and the first implementation
  requires full-root retention and exactly one configured signing authority.
- it MUST carry `source_head_root`, `source_finalized_slot`,
  `source_finalized_root`, `handoff_head`, `handoff_root`, and
  `handoff_synced_to`; numeric proof fields are presence-sensitive, so stripping
  one is invalid rather than an assertion of slot zero;
- the same document MUST contain the named finalized handoff entry at exactly
  `handoff_root`/`handoff_synced_to`.

Followers configure the expected kind per head (omission defaults to finalized)
and reject a signed kind change before fetching. Mutable bounds are additionally
limited by the follower's configured maximum window. Version-1 readers reject
versions 2 and 3; version-2 readers reject version 3. Every follower MUST be
upgraded before a writer activates a newer schema. A version-3 reader may
continue to accept version-1 finalized-only and version-2 proof-aware
authorities. `unfinalized-mutable` remains a single-authority contract even in a
version-3 document; multiple writers MUST NOT arbitrate mutable snapshots by
revision, wall clock, root coverage, or majority.

The finalized handoff is physical overlap, not a timing assumption. A mutable
generation may advance its `window_start` past slot `s` only after the selected
finalized ALL generation covers `s`. A live virtual view uses ALL whenever ALL
covers the requested slot and consults the mutable head only above that frontier;
therefore it requires `window_start <= all.synced_to + 1`. If source history or
ALL lag cannot satisfy both this invariant and the configured window bound, the
writer keeps the previous mutable generation and reports the handoff blocked —
it never publishes a gap to satisfy a size cap.

The generation request additionally carries the exact finalized root/frontier
the tracker observed. Those values are part of its idempotency digest. At commit,
the writer permits an advance only when the finalized head is still the same
in-process append lineage, has not regressed, and remains at or below the
request's trusted `source_finalized_slot`. The durable generation state keeps
both the request-observed anchor and the commit-time anchor. Later ordinary
appends in that lineage may refresh the signed handoff root/frontier up to the
source-finalized watermark without rewriting the durable commit anchor.

A real truncate/rewrite rotates the lineage and suppresses the entire mutable
side. A root-equal truncate is a no-op and preserves it. After restart, a mutable
proof may rebind only when the reopened finalized root/frontier exactly equals
its durable commit anchor; a mismatch remains unavailable even if the root later
returns (ABA). Proofless legacy state, malformed proof, quarantine, internal
gap, or a finalized advance above the source watermark removes the mutable entry
from publication and makes its physical reads retryable `503`/`no-store`, never
definitive `404`. Selecting a fresh proof-aware generation is the recovery.

Each head entry MAY carry a `manifest` field: the CID of the tip of that head's
manifest chain (10.5), the published, signed commitment to the ordered source
list the head was built from. It is appended after `dir_depth`, and is present
only for a head that has a manifest chain — a head without one omits the field
entirely (never an explicit null). Omission is what keeps this a
backward-compatible addition to the signed bytes: the ALL head (an identity
filter, nothing to attest) and every v1 head predating the manifest chain —
including the operational drill and dogfood heads — marshal to exactly the bytes
they did before, so their existing signatures still verify. Under the canonical
rule above, a verifier that round-trips the served document reproduces the field
where present and its absence where not, with no change to any other field.

### 8.1 IPNS channel

If IPNS publication is enabled, the writer additionally:

- stores the canonical JSON bytes of the publication document as a raw
  block (so both channels carry byte-identical documents and the doc itself
  is content-addressed);
- successfully advertises a recursive provider record for that exact raw-block
  CID before publishing any IPNS value that names it. If the provider write
  fails, the previous IPNS value remains authoritative and the new publication
  is retried; a writer MUST NOT advertise a document that a cold follower
  cannot discover;
- publishes an IPNS record (`boxo/ipns`) whose value is `/ipfs/<that CID>`,
  signed by the archive's libp2p key (ed25519; MAY be the same key as the
  document signing key), with a monotonically increasing sequence number;
- sends the publisher a NOTIFICATION on every root swap (coalescing is permitted,
  so this is not one publication per swap) and republishes on an interval (config;
  default 4h, record lifetime 48h). Coalescing is of pending VALUES: each attempt
  publishes the newest document pending when it starts and overwrites any earlier
  pending value. It is not cancellation of an in-flight put by a later notification,
  though -- a swap that arrives while a DHT put is already IN FLIGHT cannot stop it,
  so that (possibly intermediate) publication completes; and a buffered wakeup can
  cause a redundant republish of the same document. So a burst usually collapses to
  one publication, but an intermediate document CAN reach the DHT -- which is why a
  manifest replay must both disable IPNS and withdraw the HTTPS read route
  (operations 4.6), not rely on coalescing to hide the intermediates. Within one
  process,
  republishing an unchanged document reuses its sequence and only refreshes
  validity; a changed document takes a new number. The last-published value is
  process-local, though, so the FIRST publication after every restart takes a
  new number even for an unchanged document. A higher number is not a
  regression -- but it is only not rejected ON SEQUENCE GROUNDS: a follower
  still applies its signature, freshness, root, manifest, and other adoption
  checks (11.3), any of which can still refuse the record.

An implementation MAY split the private signing authority from a public
provider/router process. In that topology the private process sends the exact
document and already-signed IPNS record over a local authenticated transport;
the public edge still MUST complete document `Provide` before `PutValue`.
Accepting that local handoff is a commit point: cancellation after acceptance
MUST NOT be reported as a failed publication while the same live client lets
the edge complete unnoticed. Bloar therefore gives the edge transaction its own
bounded context and makes the writer wait for its structured stage result. The
budgets are strictly ordered (edge transaction < writer request < edge server
ceiling), and the two configs must agree on the transaction budget. A failed
record put may leave a newer durable sequence/revision floor for idempotent
restore, but never lowers a floor or publishes IPNS before the document
provider succeeds.

Followers MAY resolve via IPNS, HTTPS, or both; when both are configured,
take the freshest document that passes signature verification, subject to
the no-regression rule (11.3). IPNS provides authenticity, not freshness: a
withheld-update attack can serve a stale-but-valid record within its
lifetime, which is exactly what no-regression plus dual-channel resolution
mitigates.

## 9. Pinning and GC

Per-head policy, from config:

| mode | pins held |
|---|---|
| `full` | one recursive pin on the current Head root |
| `window: <dur>` | recursive pins on each sealed Segment whose window intersects `[synced_to - dur/SECONDS_PER_SLOT, synced_to]`; recursive pin on the open Segment; direct pins on the Head root, every DirNode page, and every sealed Segment block outside the window (the index stays complete under every mode; only blob retention slides) |
| `none` | direct pins on the Head root, DirNode pages, and open+sealed Segment blocks (index preserved, blobs not) |

Reconciliation runs after every root swap and on a timer (default 5m):
compute desired set, add new pins, persist ledger, remove stale pins
(including the previous root's pin in `full` mode), in that order.

**Manifest tip**: a head that has a manifest chain (10.5) carries one recursive
pin on its tip Manifest, filed under a new ledger purpose `manifest`. Because
each Manifest links its predecessor through `prev`, that single recursive pin
protects the whole chain back to genesis. It is held in every retention mode —
`full`, `window`, and `none` alike — for the same reason the index is: the chain
is a head's proof of what it selected, it is negligible next to the blobs, and a
mode that dropped it would leave the head unverifiable. A follower replicates the
tip through the ordinary reconcile-and-fetch pass (11.3), so the provenance
travels with the head.

GC is an online epoch mark-and-sweep owned by bloard. (boxo ships no GC — the
implementation that phrase suggests lives in kubo, over a full IPFS node —
and adopting boxo's pinner would duplicate pin state the ledger of 6.2 already
owns.) It uses the pin ledger as the authority without holding writers behind
the full archive walk.

At the **epoch cut T0**, GC MUST take the library-level mutation/publication gate,
reconcile every head, expire stale staging rows, snapshot the remaining pin
groups, and activate a transient protection epoch before releasing the gate.
An unflushed reconciliation would snapshot a stale root and could otherwise
sweep blocks of a published root. The gate is held across each whole refs
application, `Truncate`, ingest put, and the follower's final root/checkpoint
publication (lock order: gate first, internal locks after), so none of those
publication transitions can be half-visible at the cut.

After T0, marking and enumeration proceed while writers run. Let M be the set of
multihashes reached from the T0 pin snapshot and T the set touched through the
application blockstore after T0 while that epoch is active. GC MUST retain M ∪ T:

- the application blockstore view holds the key's guard across each read,
  existence check, or write and records a successful operation in T before
  releasing that guard;
- the collector uses an untracked blockstore view, so its own mark and enumeration
  do not add every visited block to T; and
- for each candidate outside M, its final T check and `DeleteBlock` MUST
  linearize against application protection of the same multihash. If protection
  wins, the candidate survives; if deletion wins, a later read observes absence
  and a later write may recreate it.

Both M and T MUST be keyed by multihash: flatfs is multihash-keyed and reports
every block under a raw-codec CID, so a CID-keyed set would fail to match a
dag-cbor link to the same stored block. Once enumeration is complete, GC takes
a blockstore-lifecycle barrier and deactivates the epoch, waiting for any
application block operation already inside that boundary. It need not stop a
whole publication transition at finish because enumeration and deletion are
already over. A mutation completed before the cut is represented by the
reconciled snapshot; blocks used by a root published after it are protected by
the application view. These rules apply explicitly to a truncate and a follower
adoption as well as ordinary forward refs application.

Application `AllKeysChan` is a whole-operation exception to per-key protection.
If it begins while no epoch is active, it MUST hold a lifecycle read lease until
the returned key channel is drained or its context is cancelled, and `Begin`
MUST wait. If called during an epoch it MUST fail with the active epoch ID rather
than enumerate unprotected keys. Collector GC/scrub enumeration MUST bypass this
application path and use the complete, asynchronous-error-preserving iterator.

The blockstore MUST expose both its current active epoch (zero when idle) and a
monotonically increasing collection generation. Starting an epoch increments
the generation before any deletion can occur; ending the epoch clears the
active value but MUST NOT reset the generation. The generation is a proof token:
a cached presence or closure proof from an earlier generation cannot become
valid again merely because the collector is now idle.

Before a window-policy `Truncate` publishes, it MUST `Get` and CID-validate every
distinct surviving raw reference in the rebuilt Segment and every raw descendant
of a sealed Segment newly brought inside the rewound retention window, through
the protected application view. A mere `Has` is insufficient: publication must
both reject corrupt local bytes and protect each successfully read multihash in
T when an epoch is active.

The protection epoch is in memory only. It changes no flatfs layout, CID, root,
or pin-ledger format and needs no migration. The follower's versioned
sealed-Segment verification markers are optional derived keys in the existing
Pebble namespace; their absence is valid. If the process stops mid-run, deletion
stops with it; a later run constructs fresh M and T from a new reconciled T0.

In addition to the configured recurring schedule, a running daemon MUST accept
`SIGUSR1` as an operator request for one pass through the same online collector.
The requested pass MUST serialize with scheduled GC and scrub, MUST use the
daemon lifetime context rather than any remote request context, and SHOULD
coalesce a burst of identical signals. Receipt of the signal is not evidence of
successful completion; operators use the ordinary lifecycle logs and metrics.

Mark follows IPLD links from the pin snapshot. Every DAG-CBOR target MUST be read
and CID-validated; a recursive pin expands its outgoing links, while a direct pin
validates and retains only that block. Raw targets have no links, so mark checks
that each is locally present and records its multihash without reading all of
its bytes. A missing marked target follows the per-head fail/heal rule below.
A separately scheduled **validating scrub** MUST completely enumerate flatfs
and CID-validate every object that enumeration observes, including unreachable
garbage, and report its outcome independently of reclamation. Enumeration errors
MUST fail the pass rather than masquerade as a complete scan. Objects added
concurrently after the iterator has passed are covered by a later pass. Scrub
MUST NOT delete or refetch an object.
GC and scrub MUST be serialized against each other on one store, although
ordinary application traffic continues during either pass.

The scrub is a full-byte store audit; GC success proves safe reclamation and
that marked targets existed, not that every raw object hashed correctly.

**Ingest staging**: every blob PutBlobs stores takes a direct `staging`
pin, filed in the ledger under the reserved head `_staging` (unreachable
by 3.1's name grammar), always carrying an expiry (`ingest.staging_ttl`,
default 24h; a re-put extends it). The pin drops when a refs batch
referencing the blob is applied; GC's mark includes staging pins and a
pre-mark pass expires stale rows. This closes the former
ingested-but-unreferenced sweep window.

**Follower fetch staging**: a follower's fetch pass (11.3) makes blocks
durable before the reconcile that pins them lands, so a GC in that gap used
to sweep a freshly-fetched block and leave the adoption's pin dangling — the
follower's form of window (a). It is closed the same way: the fetch pass
takes a `staging` pin on every block it fetches, with the block and under
the gate, so the block is in the mark set from the moment it is durable; the
pins drop once the pass finishes (the adopted root is durable and
registered, and GC reconciles before it marks, so the head's own pins retain
the blocks), and expire on the TTL if the pass dies first. Fetched blocks and
blocks found already present pass through the protected application view.

A follower's subtree-walk memo MUST be keyed to the blockstore's monotonically
increasing collection generation, not merely to the currently active epoch.
An ordinary retention sync captures generation G before its walk. Before it
marks the root or manifest tip fetched or drops any staging pins accumulated by
that walk, it MUST acquire the publication/GC gate and confirm that the
generation is still G. If the sync crosses a generation boundary, it MUST leave
those completion markers unchanged and stale, and MUST leave the staging pins
in place; a later poll retries under the new generation. Memo entries stamped
with an earlier generation MUST NOT suppress that retry.

The generation-stamped subtree memo proves local presence and epoch protection;
it is not a semantic-verification result. Under `verify: full`, an ordinary sync
MUST check every `RefEntry` of a Segment before recording that Segment in the
semantic memo (`verifiedSegments`), including an entry whose blob was already
present locally. A partial or protection-only walk MUST NOT record that semantic
proof. Because a Segment CID commits to all of its `RefEntry` bindings and each
blob CID commits to the verified bytes, a successfully recorded proof MAY be
reused across collection-generation changes and refetches of the same CIDs.

For a **sealed Segment**, that proof MUST be stored under a versioned Segment-CID
key in the follower's checksummed KV after every entry succeeds. It MAY use a
non-synchronous durability write: losing a recent marker causes safe extra work,
never trust without proof. A clean restart reuses surviving markers; an absent,
invalid, or unreadable marker is respectively reverified or failed closed. The
key version MUST change whenever the semantic verification rule changes. For the
single mutable-position **open Segment**, the proof MUST remain memory-only so
abandoned intermediate CIDs cannot grow the KV without bound. If an already
verified open CID later appears sealed, its complete in-memory proof MAY be
promoted to the durable sealed-Segment marker without repeating KZG work.

Whenever exposure changes a head root from A to B, the follower MUST clear the
root completion marker even if A may recur; a manifest-tip change likewise
clears its completion marker. Thus A → B → GC → A performs a new retention walk
instead of trusting A's pre-B marker after A-only descendants may have been
collected. This transition reset is separate from collection-generation scoping.

Follower adoption and checkpoint `Resume` use the same generation as a
pre-publication proof token:

- If no collection epoch is active, the transition MAY skip the retained-closure
  walk. Commit MUST acquire the gate and require the generation still to equal
  the captured token before its first durable checkpoint or head-exposure
  write. A collector cut in the intervening window therefore refuses the
  commit and causes a retry.
- If an epoch is active, the transition MUST walk through the protected application
  view the complete closure selected by that head's retention policy, together
  with the manifest chain. Blocks fetched by that proof remain staging-pinned.
  If the collection generation changes, the proof MUST be repeated under the
  new generation.
- Under the gate, commit MUST recheck every plan's generation token and
  validating-`Get` the local head root and any manifest tip before the first
  durable publication write. `Has` is not a sufficient final touch because it
  cannot reject corrupt anchor bytes. Staging pins from the adoption proof MAY
  drop only after successful exposure or reconciliation has made the new head's
  own retention pins authoritative.

The follower's transition lock remains held across the active-epoch closure
proof so another Poll, Resume, or quarantine transition cannot replace the
plan being proved. This may delay those transitions on local I/O or fetches,
but existing reads continue to serve the last committed generation and GC does
not wait on the follower's transition lock. The extended hold is paid only for
the rare generation-aware adoption or concurrent Resume which overlaps an
active collection; an ordinary startup Resume and idle adoption take the
token-only path.

A compatibility blockstore which cannot report the active epoch and collection
generation MUST instead hold the publication/GC gate across the entire retained
closure walk and completion stamp. This conservative fallback applies to both
ordinary sync, adoption, and Resume, and it MUST neither consult nor populate the
shared presence memo: without a monotonic generation, a completed collection
could make that memo stale while leaving its process alive. The production
epoch blockstore uses the generation-aware paths above. An implementation which
reports a generation but cannot report whether its epoch is active MUST
conservatively take the full closure path and validate its generation token at
commit.

**Follower mark self-heal**: when reachability reads a recursive DAG-CBOR node
or checks a raw/direct target, a block missing under a *followed* head is fetched
back and the walk continues. This repairs dangling pins left by an earlier fetch
window. The scope is per head, not per node: a missing marked block under a
*written* head is real divergence between the ledger and store and stays
fail-closed even on a node that also follows. A block shared by a written and
followed head is checked under the written head's fail-closed rule before any
followed-head walk may fetch it. CID-invalid bytes encountered in DAG traversal
fail closed and MUST NOT be overwritten as a heal. The scrub independently
reports CID-invalid stored objects, regardless of reachability or head role,
and never heals them. Collector and scrub outcomes, progress, validation, and
refetch work MUST be distinguishable in logs and metrics.

An HTTP handler which materializes archive blocks MUST take the publication/GC
gate's read side before selecting its root or manifest-tip snapshot and MUST
hold it until every block needed for the response has been read, verified, and
materialized in memory. The blobs handler MUST take this lease only after response-memory
admission, so an admission wait cannot delay T0. The manifest GET MUST take it
before selecting the tip. The lease MUST be released before the first response
`WriteHeader` or `Write`, with cancellation-safe cleanup for a path which writes
nothing; a stalled client therefore cannot delay collection. Publication MAY
replace and unpin the selected immutable snapshot while the read continues, but
the next T0 cannot omit it until materialization finishes. A read beginning
after T0 waits for the short cut and then uses the active epoch's ordinary
per-key protection. This supplies snapshot-stable HTTP responses without a
grace pin on retired generations.

Shared blobs need no special handling: GC keeps anything reachable from any
live pin.

## 10. Ingest processes

Both indexers are stateless: progress = `GET .../synced_to`.

Temporary absence of the archive is not a protocol decision. After the archive
client exhausts its bounded transport/malformed-success/HTTP-5xx retries, both
finalized indexers and the unfinalized tracker MUST stay alive, report the
archive dependency unavailable, wait one poll interval, and re-read durable
state before doing more work. The unfinalized tracker retains its last complete
selected generation and MUST NOT expose a partial replacement. An authoritative
HTTP 4xx, archive-limit mismatch, manifest or generation conflict, malformed
configuration, and every other safety/protocol failure remain terminal.

### 10.1 Beacon indexer (ALL head)

The beacon indexer runs in one of two modes, and the mode is the trust model.
Existence and absence — what a slot must contain — MUST come from a trusted
source; blob bytes may come from an untrusted one, because a versioned hash
anchors them.

**Anchored mode** (upstream is a beacon node). A trusted block feed (the beacon
node's block API) is the sole authority on existence and absence; blob endpoints
are untrusted, ordered byte sources.

```
loop:
  F = finalized slot (GET /eth/v1/beacon/headers/finalized on the block feed)
      execution_optimistic:true or 503(SYNCING) -> not a bound yet; wait
  s = archive synced_to + 1 (or origin_slot)
  for batches of up to B slots in [s, min(F, s+B-1)]:
      for each slot:
          GET block feed /eth/v1/beacon/headers/{slot}
              404 -> candidate missed slot (see continuity below); no row
              200 -> GET /eth/v1/beacon/blinded_blocks/{slot}, read
                     blob_kzg_commitments:
                  0 commitments -> verifiably blobless; no row, coverage advances
                  N commitments -> vh[i] = 0x01||sha256(commitment[i])[1:];
                     try each blob source IN ORDER (unfiltered, the whole slot,
                       unless the source is vh-keyed -> then filtered by those vhs):
                       accept iff 200 with exactly N blobs AND blob[i] commits to
                         vh[i] (KZG, positional); record the row
                       else (404, wrong count, KZG mismatch, 503, error): the
                         source cannot help -> try the next
                     all sources exhausted -> batch FAILS (never record absence)
      POST /bloar/v1/blobs (all sourced bytes)
      verify each stored vh equals the block-derived vh it was anchored to
      POST refs {rows (vhs already block-derived), synced_to = last slot in batch}
  sleep poll_interval when caught up
```

A 404 from `headers/{slot}` is **not** trusted as a missed slot on its own: a node
still backfilling historical blocks 404s a header it will later have,
indistinguishable from a genuine miss. Missed slots are PROVEN by parent-root
continuity: every present slot's `parent_root` MUST equal the `root` of the most
recent present slot before it. A present slot whose `parent_root` does not match
means the feed 404'd or is hiding a block it must have — a FATAL error naming both
slots, never absence. The anchor is carried across batches and seeded on start by
walking headers back from the resume point to the last present slot (bounded; a
long run of 404s is a still-backfilling feed, a hard error). Blobless-with-block
slots participate in the chain; only header-404 slots are skipped, and continuity
is exactly what proves those 404s real. Absence is therefore never recorded from a
blob source, and a beacon node past its blob retention is safe: it still has every
block, so it remains the authority even when it has pruned the blobs themselves.

An indexer MAY be configured with a secondary full-history blob source. It is
just a second untrusted byte source, tried after the primary; because every
candidate is anchored against the block's commitments, a fallback needs no
corroboration and a wrong or absent answer from either is caught, not trusted.
There is no unanimity rule.

A blob source (a beacon node or a Bloar archive) is asked for the whole slot. The
answer arrives in block order, which is exactly the order `vh[]` is in — one
entry per commitment, duplicates repeated — so `blob[i]` anchors positionally
against `vh[i]`.

**Mirror mode** (upstream is a bloar archive; spec 11.5 deterministic
replication). The same loop pointed at another bloar archive's
`/{head}/eth/v1/beacon/blobs/{slot}` (the read API is the backfill protocol). With
no block feed it has no independent authority on what a slot must contain, so it
COPIES the source's coverage decisions rather than re-deriving them: F = that
head's `synced_to` (7.1 serves no finalized-header endpoint; the statement is the
same, "do not read past here"). There is no block feed and no fallback. At startup
the indexer reads the upstream's `origin_slot` (GET `/bloar/v1/heads/{head}`) and
refuses to run unless it is at or below this head's origin -- the upstream must
cover the whole head to reproduce it. After that check a covered slot's answer is
classified: 200 (empty or blobs) is recorded AS GIVEN, a 503 stops the batch and
waits, and a 404 is a PROTOCOL VIOLATION (an archive 404s only below origin, which
the check excluded), never absence. KZG still anchors every INCLUDED blob to its
versioned hash, so the source cannot forge bytes; but a covered-empty 200 over a
slot the source silently omitted a real blob from is reproduced, not caught.
Completeness is therefore INHERITED from the source, not re-derived -- a re-derived
root equal to the source's proves faithful reproduction of the source's decisions,
never their completeness against the chain (11.5). An independent completeness
check must run anchored mode against a trusted block feed.

### 10.2 Chain indexer (per chain head)

The chain indexer (the `index/chain` component; v1 shipped it as the arbitrum
indexer, back when a chain head could only mean an Arbitrum SequencerInbox) fills
a per-chain head with exactly the blobs that chain's L1 posting sources named. A
head's filter is an ordered schedule of **sources** (10.4); the loop below is
that schedule reduced to its shipped default — a single `inbox-events` source
over the chain's SequencerInbox. Additional or different sources change only
which transactions the scan step selects, never the loop's shape.

Config: parent chain RPC (which MUST be trusted for `eth_getLogs`
completeness; 10.4), the head's source schedule (10.4), beacon/archive
upstream, `fetch_blobs: bool`.

```
loop:
  L = latest finalized L1 block
  b = L1 block for archive synced_to (via timestamp -> slot inverse) + 1
  scan the sources active over [b, L] (10.4):
      for each type-3 (blob) batch tx a source selects:
          vhs  = tx.BlobHashes()            # in-tx order
          slot = (block.timestamp - genesis_time) / SECONDS_PER_SLOT
          merge into row for slot (encounter order, dedup)
  if fetch_blobs: fetch exactly those vhs from upstream
                  (GET /eth/v1/beacon/blobs/{slot}?versioned_hashes=..),
                  POST /bloar/v1/blobs
  else:           require chain synced_to target <= ALL head synced_to; wait
  POST refs {rows, synced_to = slot(latest scanned finalized L1 block)}
```

Non-blob batches (calldata, AnyTrust) produce no rows; coverage still
advances. Indexers use plain go-ethereum, NOT nitro packages: nitro pins a
fork of go-ethereum via a replace directive, which would silently swap the
geth the archive's KZG/CID code builds against. Nitro imports are confined
to the separate `conformance/` module. The SequencerBatchDelivered topic
hash is pinned as a constant with a test deriving it from the event
signature string (the shipped `inbox-events` default; 10.4).

### 10.3 Finality

Indexers MUST only process finalized data (beacon: finalized checkpoint; L1
execution: `finalized` block tag). The archive itself does not verify
finality; it trusts its authenticated writers.

### 10.4 Chain-head sources

A chain head's filter is an ordered list of **sources**. The head's rows are the
UNION of what its sources select, deduplicated per row in encounter order:
sources are visited in list order, each contributes the `(slot, [vh])` rows it
selects, and a vh already present in a slot's row (from an earlier source, or
earlier within the same source) is not added again. Union with per-row dedup is
what makes overlapping sources safe: a blob two sources both select is one ref,
in the position the first encounter gave it.

A filter is a schedule and not a single rule because posting mechanisms change
across L1 history without changing what a head means. A chain that posts batches
as blob transactions to its SequencerInbox today, and later moves to sending blob
transactions to a plain EOA (a Base-style arrangement), is one head —
`arbitrum-one` is still `arbitrum-one` — described by two sources scheduled over
disjoint block ranges. The head's meaning is "the blobs this chain posted"; the
schedule is how that meaning is spelled across a history in which the "how"
changed.

Two source types ship:

- **`inbox-events`**: a contract `address` and an event `topic`. The scan reads
  that contract's logs for that topic over the source's block range and records
  the type-3 batch transaction behind each log (10.2). The shipped default is the
  SequencerInbox's `SequencerBatchDelivered`, whose `topic` is the pinned
  constant of 10.2 — derived from the event signature in a test, never carried as
  an ABI. This is the v1 filter, now named.

- **`blob-txs`**: a recipient `address` and a REQUIRED, non-empty `senders`
  allowlist. The scan records every type-3 transaction sent to `address` by a
  sender in the allowlist, over the source's block range. The allowlist is not a
  convenience and it is not optional: anyone can send a blob transaction to any
  address, so a `blob-txs` source without a sender allowlist is a write handle to
  the head that any third party on Earth holds, and the archive would faithfully
  record their arbitrary blobs as this chain's history. A head is a claim about
  what a specific sequencer posted; an unrestricted EOA source cannot make that
  claim. An empty or absent `senders` is therefore REFUSED at config load, not
  merely discouraged.

**A complete `eth_getLogs` is trusted, not verified.** An `inbox-events` scan
selects a chain's batches server-side with a single `eth_getLogs` over the
finalized range and trusts the node to return EVERY matching log; a short answer
is undetectable to the indexer. `chain.parent_chain_rpc` (12) MUST therefore be a
trusted full node — or a provider known to return complete `eth_getLogs` results
or to error, never to truncate silently. A provider that silently caps an
over-large result set drops `SequencerBatchDelivered` logs while the scan
advances `synced_to` across their blocks, and each dropped batch becomes a
permanent, absent 404 that replay and followers inherit — the same false history
a coverage gap produces (below), reached by a different route. An error is safe:
the indexer is crash-only and retries. `blob-txs` sources read full block bodies
and select nothing server-side, so they are not exposed. Implementations MAY
read those bodies concurrently and MAY batch consecutive
`eth_getBlockByNumber` calls, but MUST bound both dimensions and MUST reduce the
answers in ascending L1 block number and transaction-body order. RPC completion
order is not encounter order and MUST NOT affect refs or the resulting root.

Each source carries `from_block` and an optional `until_block`, both INCLUSIVE.
`until_block` absent means the source is open-ended. The block ranges of
different sources MAY overlap; union-with-dedup makes overlap harmless, so a
migration need not compute an exact hand-off block to avoid double- or
under-counting.

**Contiguity.** A schedule MUST leave no uncovered block range BETWEEN its
sources: the union of the source ranges MUST be contiguous from the earliest
`from_block` onward. Overlap is the supported way to hand off between two
sources; a GAP is not. A block that no source covers is not neutral, because the
scan advances the head's `synced_to` across it while selecting nothing (10.2):
every batch a chain posted in an uncovered block would be recorded as a
permanent, absent 404, and replay and followers would inherit that false
history. The single unprotected boundary is therefore an off-by-one at a
close-and-add (source A `until_block` 1000, source B `from_block` 1002 leaves
block 1001 uncovered), and it is REFUSED — both at schedule validation and on the
manifest upgrade path — rather than served wrong. A gap BEFORE the earliest
source is not a hole: the schedule simply begins there.

**Immutability.** A source entry is immutable once it has covered ground — once
the head's position (`synced_to`, mapped back to an L1 block by the timestamp
inverse of 10.2) has entered its range. To change a source that is still open
(most often to change a `senders` allowlist), you do not edit it: you **close and
add**. Set `until_block` on the existing source to the last block it should
govern, and add a new source that begins at the next block with the changed rule.
Editing an already-covered source in place would silently rewrite the meaning of
history the head has already served — coverage is a claim followers and
re-derivers have already consumed — and a head whose past can be rewritten is
exactly what the manifest chain (10.5) exists to prevent. Close-and-add expresses
every change as a new range appended ahead of the position, which is always
legal.

### 10.5 The manifest chain

A filter that can be restated after the fact is worth no more than the honesty of
whoever restates it, and 11.5's audit primitive — two independent runs over the
same finalized data produce byte-identical roots — says nothing at all unless the
filter that chose the rows is itself fixed and public. The **manifest chain**
fixes it: it makes a head's source schedule as content-addressed, published, and
append-only as the head's data, so "trust the filter" reduces to "trust a CID",
exactly as "trust the data" already does.

**Manifest object.** A Manifest is a dag-cbor block (2), encoded by the same
canonical rules as every other block:

```
Manifest = {
  "v":          uint,            # manifest schema version
  "head":       string,          # head name (matches the Head's "name")
  "sources":    [ Source, ... ], # the full ordered source list as of this manifest
  "prev":       link | null,     # previous Manifest; null only for the genesis manifest
}

Source = {
  "type":       string,          # "inbox-events" | "blob-txs"
  "address":    bytes20,         # inbox contract (inbox-events) or recipient EOA (blob-txs)
  "topic":      bytes32,         # inbox-events only: the event topic0
  "senders":    [ bytes20, ... ],# blob-txs only: the non-empty allowlist (10.4)
  "from_block": uint,            # inclusive
  "until_block": uint,           # inclusive; the key is ABSENT for an open-ended source
}
```

`sources` is the WHOLE schedule as of this manifest, not a delta: a Manifest is a
self-contained statement of the head's filter at a point in its history, and
`prev` chains it to the statement it replaced. `address`, `topic`, and each
`sender` are CBOR byte strings, not links; only `prev` is an IPLD link (tag 42),
so a recursive pin on a tip Manifest walks the entire chain to genesis. A
type-specific key that does not apply to a source's `type` is absent, not null;
likewise `until_block` is ABSENT — never an explicit null — for an open-ended
source, and a decoder MUST reject a null there. The reason is the one that makes
these blocks addressable at all: canonical encoding demands exactly one byte
representation per meaning, so two legal spellings of the same source list would
give one manifest two CIDs and silently break the compare-and-swap and the chain
equality the whole scheme rests on. This is the deliberate opposite of Head's
`synced_to: null` (3.1), where an explicit null is live mutable state and its
explicitness aids debugging; a manifest is frozen history, and there uniqueness
wins.

**The tip is published and pinned.** The CID of the newest Manifest — the tip —
is published in the head's publication-document entry as its `manifest` field (8)
and carried by a recursive pin under the ledger purpose `manifest` (9). The head's
Head object (3.1) does NOT link the manifest: the head root stays a pure function
of the filtered data, so re-derivation reproduces it, while the manifest chain is
the separately-addressed provenance of the filter that selected that data. The
two are published together and verified together, but neither is nested in the
other.

**Upgrade is a compare-and-swap on `prev`, bound to a generation.** To change a
head's schedule, the writer builds a Manifest whose `sources` is the new full
schedule and whose `prev` is the CURRENT tip CID, and submits it to the
authenticated `POST /bloar/v1/heads/{head}/manifest` endpoint (7.2) together with
an `expected_head_root`: the head root the writer validated the upgrade against.
The server accepts it only if, atomically under the head lock, `prev` equals the
tip it currently holds AND `expected_head_root` equals the head's current root;
it then makes the new Manifest the tip. A stale `prev` or a stale
`expected_head_root` is a `409`. The generation binding closes the gap between
validating and publishing: the head root is the head's generation id, so a refs
commit landing between the writer's validation and this POST — one that could
move the position and make a formerly-legal change rewrite newly-covered ground —
advances the root, the compare fails, and the writer re-validates against the
advanced head rather than publishing against a position that has moved.

**Division of enforcement.** The server enforces only what a node with no L1 view
can: the manifest is structurally well-formed, `prev` is the current tip, and
`expected_head_root` is the current root. It does NOT check that the new schedule
is a legal append-only successor, because it cannot — "legal" is a statement
about where the head's position sits on L1, which only something watching L1 can
evaluate. That full check is the chain indexer's, run as an authenticated
**preflight** BEFORE the POST (10.2, and `bloar-index publish-manifest`): mapping
the head's current position (`synced_to`) to an L1 block via its RPC, it loads and
decodes the actual predecessor and requires that every rule applying to blocks AT
OR BEHIND that position is UNCHANGED, and only rules strictly AHEAD of the
position may differ. That is the formal content of 10.4's immutability rule: the
past a head has covered is frozen, the future is open, and close-and-add is legal
precisely because it only ever adds ranges ahead of the position. Neither side
assumes the other's check: the server never trusts a schedule to be legal, and
the preflight never assumes the tip will still be current at publish time — the
`prev` and generation compares are what make the preflight's verdict binding.
Followers enforce a weaker, L1-free version of the same append-only property
through a manifest-ancestry floor (11.3): they cannot judge whether a change ahead
of the position is legal, but they can and do refuse a tip that discards the chain
they already hold.

**Refs are commit-bound to the tip.** Point-in-time validation is not enough on
its own: a schedule can be legal when a process starts and the tip can move under
it, or a process can keep running across a legitimate handoff. So every refs
request (7.2) carries an `expected_manifest` — the tip the batch was scanned under
— which the server compares to the head's registered tip atomically with the
commit, under the same head lock. It is REQUIRED for a head that has a manifest
chain and FORBIDDEN for a chainless head (the ALL head, and any head predating the
manifest chain); a mismatch is a `409` carrying the current tip, and the writer
stops and resyncs. A chain indexer therefore commits nothing across a tip change:
the schedule that selected a batch's rows is the schedule the archive still
attests when the batch lands, or the batch is refused. This is what makes 11.5's
re-derivability hold at the commit, not merely at startup.

**Recovery order.** Because the check is anchored to the position, the only way to
legally change a rule the position has already passed is to move the position back
first — which is exactly what `truncate` (5.4) does. This makes the recovery
sequence mechanical rather than a convention: to correct a source that has already
covered ground (a wrong allowlist that admitted a bad blob, say), truncate the
head to before the affected range, publish the corrected manifest (now legal,
because the truncated position sits behind the changed rule), then resync.
Manifest-first is rejected by the indexer's check; truncate-first is what makes it
pass. Operational detail for the deep-truncate case — where coverage falls below
the immutable horizon (7.1) and cached responses must be purged — is in the
runbook ([operations.md](operations.md)).

**Startup binds the runtime schedule to the tip.** A chain indexer verifies, once
at startup, that its configured schedule EXACTLY equals the published tip, and
records that tip to bind its refs (above). Exact equality, not append-only
succession: the indexer must run precisely the schedule its tip attests, so the
tip each batch is bound to is the schedule that batch was scanned under. A config
that is a legal FUTURE successor of the tip is refused until its own manifest is
published (via the preflight) and the config then equals the new tip — which is
what stops a future-divergent runtime schedule from being accepted before
activation and then crossing its own unpublished boundary. A chain indexer whose
head has NO manifest tip refuses to run rather than writing an unattestable head:
its selection is only verifiable bound to a manifest.

**Backward compatibility and bootstrap.** A head with no manifest chain is still
valid to SERVE and to FOLLOW — exactly the status of every v1 head, the ALL head
(an identity filter, nothing to attest), and any head predating the manifest
chain — but a chain indexer will not build one without a manifest, so a chain head
is bootstrapped genesis-first: publish a **genesis manifest** whose `sources`
describes the intended filter (or, for an existing v1 head, the filter it was
already built with) and whose `prev` is null — the compare-and-swap from no-tip to
tip — then run the indexer against the matching config. From that point the head's
filter is attested and append-only; a head served without one simply makes no
claim a third party could check.

## 11. Deployment roles and distribution

### 11.1 Roles

Each physical head has **exactly one mutation writer**: the bloard that owns its
local selector and generation state, runs the mutation engine (section 5),
accepts ingest (`POST blobs` / `POST refs`), and signs and publishes that copy's
heads document. A version-3 logical archive MAY have several independent
physical writers, with separate stores, signing keys, publication revisions,
IPNS names, and URLs, publishing compatible copies of the same finalized head
under one `archive_id`. They never share mutable state and followers compare
their claims only by the finalized content partial order in section 11.3.
`unfinalized-mutable` remains single-authority.

All other nodes are **followers**: they replicate published heads over IPFS and
serve the read API. A follower runs no indexers, no ingest API, and no mutation
code. One bloard MAY be writer for some heads and follower for others (`role` is
per-head via config).

The writer does not need to be publicly reachable for reads: followers can
serve the entire public API while the writer exposes only libp2p and the
heads document.

The standalone Kubo archive replica is a restricted follower deployment. It
MAY authenticate and retain selected published heads in full through an
operator-owned Kubo node and MAY expose the section 7 public GET surface from
that committed generation. Kubo remains the block store, libp2p host, pin
database, and GC authority; the controller owns only its bounded
replay/checkpoint metadata and named generation-anchor pins. Its operational
contract is [`kubo-replica.md`](kubo-replica.md). It MUST NOT be treated as a
writer or as the removed exclusive managed-Kubo backend.

The replica HTTP surface is disabled by default. When enabled it MUST mount no
mutation route and possess neither an ingester nor an API token. Reads MUST use
Kubo's authoritative local-only block operation (`offline=true`), MUST NOT
initiate Bitswap or otherwise fetch on a public miss, and MUST resolve only
heads adopted from the current protected generation. Kubo-local absence is
retryable unavailability; local CID mismatch is an integrity failure. Reads
share the follower/retention reader gate so an old generation is not unpinned
while a request which selected it is still materializing blocks.

A standalone replica selecting an `unfinalized-mutable` head MUST explicitly
pin its expected kind, signing authority, handoff name, and maximum complete
window. When the authenticated global finalized witness is not itself selected,
the replica MUST select a finalized overlay and prove from the same signed
document that it reaches the mutable window boundary. The metadata-only witness
MUST NOT enter the generation anchor. These proof and gap checks occur before
retention changes and again from the durable checkpoint on restart; one exact
generation pin protects the complete selected finalized/live pair.

### 11.2 Block exchange (p2p)

bloard runs a libp2p host with bitswap, in both roles:

- **server**: any node (writer or follower) serves blocks it has to peers by
  default; `p2p.bitswap.serve: false` is the explicit opt-out.
- **client**: a follower's blockstore is bitswap-backed; a locally missing
  block is fetched from configured peers. Client fetching remains available
  when serving is disabled.

Bloar passes every Bitswap server work bound explicitly rather than inheriting
Boxo defaults: queued wants per peer, outstanding response bytes per peer, send
workers, engine task workers, blockstore workers, and maximum CID bytes. These
are queue/concurrency/working-set limits, not a per-peer bandwidth-rate limit.
The queue, byte watermark, and worker counts bound independent stages and have
no numeric ordering requirement. The CID limit MUST be at least 36 bytes, the
encoded length of Bloar's CIDv1 raw/dag-cbor sha2-256 identifiers.

`max_outstanding_bytes_per_peer` is a soft Boxo scheduler watermark, not an
exact hard byte ceiling: the task that takes active work to or past the
watermark is allowed to finish, after which that peer receives no new work
until earlier tasks drain. A response may therefore overshoot the watermark by
one block task. Boxo exposes the accepted per-peer wantlist, but no stable
public per-peer active-work counter, so deployments MUST treat this setting as
backpressure rather than as a memory-accounting primitive.

Peering is static in v1: followers dial the `multiaddrs` from the
publication document plus any `p2p.peers` from config. No DHT is required
for block exchange (the DHT is used only for IPNS, when enabled).

### 11.3 Follower protocol

```
admission loop:
  resolve publication doc (HTTPS and/or direct IPNS or one-hop DNSLink, section 8)
  authenticate the configured or DNSLink-delegated signer, then verify signature
  for each followed head:
      legacy finalized: reject synced_to / updated_at regression
      revisioned: reject revision replay/equivocation or kind mismatch;
                  mutable coverage may retract only at a higher revision
      reject if IPNS sequence regressed                 # transport floor
      adopt the complete new generation and commit its retention intent atomically
  signal retained-closure sync with one coalescing dirty bit
  wait until the next poll_interval boundary

single retained-closure worker:
  wait for the dirty bit
  for each configured head, in deterministic order:
      snapshot the currently admitted root and manifest tip
      run the retention fetch/verification pass
      stamp completion only if that same generation is still current
```

The daemon separates authenticated publication admission from retained-closure
synchronization. Admission is attempted immediately and on the configured poll
cadence; a slow fetch pass MUST NOT delay the next admission attempt. There is
exactly one synchronization worker and at most one pending dirty bit. A
publication admitted while that worker is busy does not create another worker
or enqueue a historical revision. It coalesces into the pending bit, and the
next pass reads the then-current generation.

An in-progress pass MAY finish useful content-addressed work after a newer
generation is admitted, but its completion stamp MUST compare the snapshotted
root and manifest tip with the current generation. A superseded pass cannot
mark a newer generation fetched. The pending dirty bit causes the worker to
revisit current state after the old pass returns. Implementations SHOULD NOT
cancel every pass merely because a newer publication arrived: when publication
cadence is shorter than closure-walk time, cancellation on every revision can
starve retention synchronization indefinitely.

This scheduling split does not weaken admission. Structural loading, signature
and contract checks, replay and ancestry floors, active-GC closure protection,
durable checkpointing, retention prepare/commit, and atomic registry adoption
remain in the admission transaction. Those checks may themselves take longer
than `poll_interval`; the guarantee is that post-admission retained-closure
work does not add another serial delay.

The exported library operation `Follower.Poll(ctx)` retains its synchronous
contract: it performs one admission attempt and then one retained-closure pass
before returning, while sharing the same single-sync permit as the daemon
worker. This keeps one-shot callers and tests deterministic without permitting
overlapping closure walks.

Implementations expose the scheduling boundary separately: admission duration
and last-success time; per-head sync duration and last completed-generation
time; whether the single sync permit is active; and the count of wakeups
coalesced into an already-pending dirty bit. Sync outcomes distinguish
`completed`, `noop`, `superseded`, and `error`; only `completed` advances the
per-head sync last-success timestamp.

The split applies to embedded-retention followers, where Bloar owns the local
retained-closure walk. An external-retention follower must prepare its
generation with the external store before admission can expose it; its
background sync pass is therefore a no-op, and no per-head sync last-success
series is expected for that mode. Admission telemetry includes that external
retention cost.

Direct URL/IPNS followers MUST configure a pubkey and reject documents whose
embedded pubkey differs from it. A DNSLink follower MAY omit the pubkey: it
accepts exactly one `/ipns/<name>` TXT target with no suffix, authenticates the
IPNS record and exact raw-document CID, and only then accepts the document's
self-signing ed25519 key. That name/signer delegation is committed atomically
with the admitted document. HTTPS may use the last committed delegated signer,
but may never bootstrap or rotate it. Configuring a pubkey with DNSLink pins the
signer while still permitting the DNS owner to rotate the IPNS name.

DNSLink and direct IPNS are mutually exclusive name authorities; HTTPS may be a
redundant data channel beside either. IPNS sequence floors are keyed by name in
a bounded MRU set, so a new name may start at a lower sequence while rotating
back retains its prior floor. Global document/per-head/manifest/coverage floors
remain continuous across every name. The default system resolver does not
authenticate DNSSEC; an explicit signer pin is the stronger trust posture.
`updated_at` equality is not a regression (the field is second-precision); an
authenticated IPNS record raises its own name's sequence floor even when its
document loses the freshness contest.

A writer's legitimate `truncate`-and-resync (5.4, 10.5) needs no follower-side
handling beyond these same floors. The truncated document, whose `synced_to` dips
below the floor, is declined as a regression like any other — and so is every
document until coverage climbs back past the floor, at which point the follower
adopts normally. The blocks below the truncation point it already holds stay
valid: truncate only drops coverage and rebuilds the spine above the cut, so the
segments beneath keep their CIDs, and the manifest recovery order (10.5)
guarantees the data re-synced above the cut is the corrected history, not the old
one.

**Manifest-ancestry floor.** A follower that has accepted a head's manifest tip
(10.5) persists that tip as a floor — the same KV family as the `synced_to`,
`updated_at`, and IPNS-sequence floors — and REFUSES a newly published tip whose
`prev`-lineage does not walk back through it. This is a pure hash-chain check:
follow `prev` links from the new tip and require the held floor to appear; no L1
and no manifest decode are involved, since the follower already holds the whole
chain as locally pinned blocks (9) and reads it by the same generic link
traversal that pinned it. Without the floor a writer could swap a head's entire
filter history for a freshly minted chain and its followers would dutifully re-pin
the replacement; with it, rewriting the filter's past is refused by the writer's
own followers exactly as a `synced_to` regression is, so the manifest chain's
append-only guarantee is enforced at the edge and not only at the writer. The walk
is cheap: manifests change rarely, the chain is short, and it is already resident
locally.

#### 11.3.1 Logical archive identity and finalized multi-writer order

`archive_id` is a stable namespace for claims about one logical archive. It is
not a credential, signer fingerprint, key roster, source locator, or consensus
vote. Operators generate one random nonzero 32-byte value, configure that same
public value on every independent writer of the logical archive, and retain it
across signer, URL, IPNS-name, and source-membership changes. Followers still
authorize each writer key through local configuration or an authenticated
delegation. Matching `archive_id` values do not make an unknown key trusted;
different values make the claims unrelated even if all other fields match.

The initial multi-writer contract applies only to `finalized-monotonic` heads.
The immutable logical-head identity is the tuple `(archive_id, net, name, kind,
origin_slot, seg_bits, fanout_bits)`. `dir_depth` is an encoded property of a
particular root, not logical identity; every candidate root must nevertheless
decode and reproduce all of its own signed publication fields before it can be
compared. A kind or immutable-tuple mismatch is unrelated/incomparable, not a
newer claim.

For two individually valid and authorized claims of that identity, followers
classify the archive roots as follows:

1. At equal coverage, the root CIDs MUST be equal. Different roots are
   cryptographic evidence of a conflicting history.
2. At unequal coverage, deterministically truncate/project the higher-coverage
   root to the lower claim's `synced_to` (or to empty coverage). The resulting
   CID MUST equal the lower root. A mismatch is a conflict; equality proves the
   higher root is an append-only extension of the lower root. Projection creates
   only deterministic content-addressed index blocks and MUST confine them to a
   private write overlay (as the reference implementation does), or run behind
   an equivalent staging/GC protection boundary.
3. Manifest tips are a second partial order. Two absent tips or two equal tips
   are equivalent: equal content IDs are already the identity proof and do not
   require loading the shared block merely to compare them. A tip which reaches
   the other by `prev` links dominates its ancestor. Two present, fully readable
   tips with neither descending from the other are a conflicting filter history.
   Exactly one present tip is incomparable until an explicit
   manifest-migration contract exists.
4. Root coverage and manifest ancestry compose only when their directions agree
   or one dimension is equal. If one writer has more archive coverage while the
   other has the descendant manifest, neither complete claim contains the other
   and they are incomparable.

A missing root or manifest block, timeout, decode failure caused by incomplete
transfer, or other storage/I/O error means **not yet proven**. It MUST NOT be
reported as conflict. The follower retries after fetching the proof material.
A conflict is reserved for incompatible content established from available,
validly signed claims and fully evaluated proofs.

Across any number of successfully resolved and proven writer observations, the
follower adopts only the unique maximal equivalence class under this partial
order. It MUST NOT use signer-local `revision`, `updated_at`, IPNS sequence,
source-list order, arrival order, or a majority vote to break a cross-writer
tie. An unavailable source or a candidate whose proof blocks are not yet
fetchable is unhealthy for that poll and does not by itself delay a compatible,
proven advance from another source. If the proven observations conflict or are
incomparable, or no candidate can be proven, the follower keeps serving its last
durably admitted generation and surfaces the condition. Cross-writer conflict
is distinct from one signer's same-revision equivocation: it does not quarantine
or withdraw a previously good finalized head. An admission checkpoint commits
the selected claim, its logical identity, source/signer provenance, proof floors,
and retained generation atomically.

Writer-key membership is likewise local policy, not part of `archive_id`. A
planned rotation adds the new key, observes an equivalent or dominating claim
under the same archive ID, then removes the old key. Removing a compromised key
prevents future admission but does not retroactively invalidate a generation
already admitted from it; operators must inspect the last trusted frontier and
explicitly recover if necessary. Changing `archive_id` creates a different
logical archive and is never a substitute for key rotation.

This mechanism provides writer-outage tolerance, not Byzantine consensus. Any
one authorized writer can publish a structurally valid append containing false
future claims, and no clock, revision, or signer count can prove which external
history is true. CID checks prevent block substitution, prefix projection
prevents rewriting already compared coverage, and `follow.verify: full` checks
blob commitment consistency, but none establishes external chain truth. A
deployment needing Byzantine fault tolerance requires a separate quorum or
consensus protocol outside this publication contract.

Pin reconciliation drives sync: the reconciler writes desired pins to the
ledger, and a **fetch pass** walks the desired recursive pins over the
bitswap-backed blockstore, fetching whatever is missing — so the per-head
pin policy determines exactly what a follower fetches and retains, and
the fetch set is GC's mark set by construction. Index blocks (Head,
DirNodes, open + sealed Segments) are always pinned directly and fetched
eagerly under every policy, so a follower answers 404 vs 503 exactly like
the writer without holding blobs.

The reconcile that pins an adopted root is asynchronous, so every block the
fetch pass makes durable is briefly unreferenced. The pass takes a `staging`
pin on each such block to keep a GC in that gap from sweeping it, and drops
them once it finishes (§9); a follower's GC repairs any pin that gap left
dangling by fetching the block back rather than failing the run (§9).

### 11.4 Read misses and verification

On a follower, a lookup that resolves in the index but whose blob block is
not local (not yet fetched, or outside the local pin window) triggers an
on-demand bitswap fetch bounded by `follow.fetch_timeout` (default 5s);
failure -> 503 (7.1). Fetched-on-demand blocks are cached but unpinned
(swept by GC unless a policy reaches them).

`follow.verify` modes:

- `cid` (default): multihash verification on every fetched block --
  inherent to IPFS; index structure and blob bytes cannot be forged.
- `full`: additionally recompute the KZG commitment of every blob the
  follower **fetches or serves** and check the derived vh against the
  index entry that named it. (Serve-time too: a shared blockstore on a
  mixed writer/follower node can satisfy a followed head's CID with a
  locally-ingested blob whose binding was never checked.) A mismatch is
  evidence of a malicious writer: QUARANTINE the head — its beacon reads
  answer 503 with `Cache-Control: no-store` and no Retry-After (nothing
  changes without an operator), it leaves the publication document and
  `/bloar/v1/heads`, and re-adoption is refused thereafter (a fresher
  signed document does not clear quarantine; the signature is what is in
  question). Mutation endpoints answer 403 on a followed head (one writer
  per head; no credential makes this node it) and 404 on a quarantined
  one. Quarantine need not be persisted: with verification on every
  serve, a restart re-detects it deterministically on first read.

The writer's signature vouches only for **completeness and freshness** of a
head; data integrity is enforced by content addressing and (ultimately) by
Nitro's own KZG check on every blob it consumes.

### 11.5 Bootstrapping, forking, promotion

- **Follower bootstrap**: configure `follow.{url,ipns,pubkey}` + pin
  policies; the protocol above backfills per policy. Verification per 11.4.
- **Deterministic replication** (mirror mode): run a beacon indexer against an
  existing bloar archive's read API as upstream (the read API is the backfill
  protocol). KZG verification at ingest is inherent; the resulting head root MUST
  equal the source's root for the same parameters. This is a determinism proof,
  NOT a trustless honesty check: with no independent block feed the replica reads
  ONLY the source archive's per-slot answers (its `origin_slot`, its `synced_to`,
  and each slot's 200/404/503) and copies them. It re-applies no filter, imports no
  **manifest chain**, and scans no L1 -- whatever the source selected, filtered head
  or ALL head alike, is reproduced as served. So a matching root proves the replica
  faithfully reproduced the source's decisions and that every INCLUDED blob is
  KZG-valid -- never that the source omitted no real blob. Completeness is INHERITED
  from the source.
- **Independently auditing an archive** (the honesty check): to verify an
  archive's completeness rather than reproduce its choices, do not use it as the
  existence authority -- use it purely as a blob-byte source and take existence
  from somewhere independent. Re-derive the **ALL head** in **anchored mode**
  (10.1) against an INDEPENDENT finalized block authority (a beacon node you
  trust): the block feed decides what every slot must contain, the archive under
  audit supplies only bytes, and a real blob the archive silently dropped surfaces
  as an all-sources-exhausted batch failure rather than being copied. A re-derived
  root equal to the archive's then means the archive is complete against that
  independent authority, not merely self-consistent. A **filtered** (chain) head is
  audited the same way with one addition: the filter is a parameter, so re-derive it
  by independently re-scanning finalized L1 under the head's **published manifest
  schedule** (10.5). This is where the manifest chain earns its keep -- it is what
  makes "the same filter" a CID two auditors can agree on rather than a hand-set
  guess, so honest auditors reproduce the same head. But a schedule CID only fixes
  the filter that was ADVERTISED; it is not proof the schedule names every real
  posting source. Filtered-head completeness is therefore proved RELATIVE TO the
  advertised schedule -- a posting the schedule itself omits is out of scope by
  construction, which is the honest boundary of what a filtered audit establishes.
  The ALL head needs no manifest: the identity filter is not a parameter.
- **Forking**: every published root is a CID; anyone holding the blocks can
  continue building from any root under a new key, structurally reusing all
  existing segments and blobs. A bad or dead writer is routed around by
  followers choosing a different name to follow; which key to trust is
  out-of-band (DNSLink gives a stable, rotatable human name).
- **Promotion** (writer failover): point indexers at a follower, give it a
  signing key, and set `role: writer`. A legacy v1/v2 takeover preserves the old
  signing key; a v3 replacement may instead use its own newly authorized writer
  key while retaining the logical archive's `archive_id`. The immutable DAG
  blocks are all present, but the promoted head's current root and manifest tip
  are NOT in the DAG: they come from the follower's authoritative `f` checkpoint
  (11.3), which promotion reads and materializes into the durable `h`/`m`
  selectors and then retires -- atomically, before opening the head. Promotion
  therefore DEPENDS on that non-DAG checkpoint; `bloard rebuild` restores only
  the blob catalog (section 6), never roots, tips, the IPNS sequence, or follower
  floors. Indexers resume from the head's `synced_to`.

## 12. Configuration reference (YAML)

```yaml
net: mainnet
beacon:
  genesis_time: 1606824023
  seconds_per_slot: 12
  genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
  genesis_fork_version: "0x00000000"
  spec_extra: {}                 # extra /eth/v1/config/spec passthrough keys
                                 # (scalars only; SECONDS_PER_SLOT not allowed here)

store:
  path: /var/lib/bloar
  gc_interval: 24h               # online reachability + reclamation (9)
  scrub_interval: 168h           # read-only full blockstore CID validation (9)
  node_cache_mb: 256

server:
  listen: ":8550"
  auth_token_file: /etc/bloar/token
  max_put_blobs: 64
  max_query_hashes: 128            # 7.1; per-request versioned_hashes cap,
                                   # duplicates counted
  max_response_bytes_in_flight: 1073741824  # 7.1; process-wide response-memory
                                            # budget, bytes
  immutable_horizon_slots: 7200
  public_read_admission:           # weighted request-cost units; default on
    enabled: true                  # false is the explicit bypass
    global_rate: 4096              # units/s, provisional pending rollout load
    global_burst: 16384            # units; must admit 1 + max_query_hashes
    client_rate: 1024
    client_burst: 4096
    client_buckets: 4096           # hard cap on the client-bucket LRU
    client_bucket_ttl: 15m
    trusted_proxy_header: ""       # e.g. X-Forwarded-For; requires CIDRs
    trusted_proxy_cidrs: []        # canonical networks; host bits rejected
  metrics_listen: ""               # 7.4; "" = disabled

ingest:
  staging_ttl: 24h                 # staging-pin expiry (section 9)

publish:
  archive_id: ""                             # optional; 64 lowercase hex chars;
                                             # shared by independent writers of
                                             # one logical archive; enables v3
                                             # and requires signing_key_file
  signing_key_file: /etc/bloar/ed25519.key   # optional
  ipns: false                                # 8.1; requires p2p.listen or
                                             # p2p.peers (announce alone does
                                             # not enable a host)
  ipns_republish: 4h

p2p:
  listen: ["/ip4/0.0.0.0/tcp/4001"]
  peers: []                        # static peers: bitswap peering AND DHT
                                   # bootstrap (one key serves both in v1);
                                   # also dialed in addition to published
                                   # multiaddrs when following
  announce: []                     # multiaddrs to put in the publication doc;
                                   # /p2p/<peerid> is appended to any that omit
                                   # it (the full form is accepted too)
  identity_key_file: ""            # ed25519 hex (same format as
                                   # publish.signing_key_file); created 0600
                                   # on first run, PeerID logged; default
                                   # <store.path>/p2p.key -- the identity must
                                   # be stable across restarts or published
                                   # multiaddrs and the IPNS name go stale
  relay:                            # default-on bounded control plane
    service:                        # active only while observed Public
      enabled: true
      reservation_ttl: 1h
      max_reservations: 32
      max_circuits_per_peer: 4
      buffer_size_bytes: 2048
      max_reservations_per_ip: 8
      max_reservations_per_asn: 16
      circuit_duration: 2m
      circuit_data_bytes: 131072    # per direction; no blob-data fallback
    hole_punching: true
    static_candidates: []          # full direct .../p2p/<peerid> addrs;
                                   # non-empty enables bounded AutoRelay
  bitswap:
    serve: true                     # false disables serving, not client fetch
    max_queued_wants_per_peer: 1024
    max_outstanding_bytes_per_peer: 1048576
    send_workers: 8
    engine_task_workers: 8
    blockstore_workers: 128
    max_cid_bytes: 168

heads:                             # heads this node WRITES
  all:
    origin_slot: 8626176
    seg_bits: 9          # 512 slots/window; see sizing note
    fanout_bits: 8
    pin: { mode: full }
  arbitrum-one:
    origin_slot: 8626176
    seg_bits: 13         # 8192 slots/window
    fanout_bits: 8
    pin: { mode: full }
    # pin: { mode: window, duration: 720h }
  unfinalized:
    kind: unfinalized-mutable
    handoff_head: all
    max_window_slots: 64
    origin_slot: 8626176            # bootstrap value; complete generations
                                    # replace the bounded window origin
    seg_bits: 5
    fanout_bits: 8
    pin: { mode: full }             # required for mutable generations

live_heads:                         # local-only virtual serving views (7.1)
  live:
    finalized_head: all
    unfinalized_head: unfinalized
  # A chain-filtered finalized archive may share the global bounded tip. Its
  # provisional path is exact-hash-only; finalized slots remain enumerable.
  arbitrum-one-live:
    finalized_head: arbitrum-one
    unfinalized_head: unfinalized
    require_versioned_hashes: true

# follow:                          # heads this node FOLLOWS (11.3)
#   url: https://archive.example.org
#   ipns: k51q..                   # direct name authority; OR dnslink, not both
#   # dnslink: swarm.example       # one hop to /ipns/<name>; signer may rotate
#   pubkey: "<hex ed25519>"        # required unless dnslink delegates signer;
#                                  # with dnslink, setting this pins the signer
#   poll_interval: 60s
#   fetch_timeout: 5s
#   verify: cid                    # cid | full
#   heads:
#     arbitrum-one:
#       pin: { mode: window, duration: 720h }   # REQUIRED, no default: the
#                                               # writer's `full` default would
#                                               # silently retain the whole
#                                               # archive
#
# Multi-writer alternative to url/ipns/dnslink/pubkey above (11.3.1):
#   archive_id: "<64 lowercase hex>"
#   sources:
#     writer-a:
#       url: https://writer-a.example.org
#       ipns: k51q...
#       pubkey: "<64 lowercase hex ed25519 public key>"
#       heads: [arbitrum-one, unfinalized]
#     writer-b:
#       url: https://writer-b.example.org
#       ipns: k51q...
#       pubkey: "<64 lowercase hex ed25519 public key>"
#       heads: [arbitrum-one]
#   source_set:
#     revision: 1
#     acknowledge_digest: "sha256:<canonical roster digest>"
#   # migrate_legacy_source: writer-a  # explicit first conversion only
```

**Segment sizing rule**: choose `seg_bits` so the worst-case sealed segment
stays under ~1.5 MiB with a ~768 KiB typical target, at ~80 bytes per blob
reference. For the ALL head at the mid-2026 BPO schedule (max 21 blobs/slot):
`2^9 slots * 21 * 80B ~= 860 KiB` worst case. For a chain head, estimate
refs/slot from the chain's posting cadence. Revisit defaults when blob
limits rise; existing heads keep their parameters (rebuild to change).

Indexer configs (separate processes) carry their upstreams, the archive URL,
token, head name, and `fetch_blobs`. A chain indexer (the `index/chain`
component; v1 shipped it as the arbitrum indexer, and a single `sequencer_inbox`
field was its whole filter) additionally carries its head's ordered source
schedule (10.4):

```yaml
chain:
  parent_chain_rpc: https://l1.example.org
  sources:                          # ordered; rows are the dedup'd union (10.4)
    - type: inbox-events
      address: "0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6"   # SequencerInbox
      topic:  "0x7394f4a19a13c7b92b5bb71033245305946ef78452f7b4986ac1390b5df4ebd7"
      from_block: 0                 # inclusive
      # until_block: 21000000       # inclusive; omit for an open-ended source
    - type: blob-txs                # a later EOA-posting era of the same chain
      address: "0x5050...sequencerEOA"
      senders: ["0xa4b0...batchPoster"]   # REQUIRED, non-empty (10.4)
      from_block: 21000001
```

The shipped default — a single `inbox-events` source over the chain's
SequencerInbox — is the v1 filter, and its genesis manifest (10.5) is what turns
that configured filter into a published, append-only claim.

## 13. Conformance and testing

1. **Nitro client conformance** (the flagship test): a Go test in a separate
   module (`conformance/`, own go.mod importing
   `github.com/offchainlabs/nitro`) runs `headerreader.BlobClient` --
   `Initialize()` then `GetBlobsBySlot` -- against an in-process `bloard`
   with known fixtures. Passing this means Nitro can sync from us.
2. **CID stability golden vectors**: fixed logical objects (Head, Segment,
   DirNode, incl. edge cases: empty, trailing nulls, single-row) with
   asserted CIDs, committed as testdata.
3. **Ported prototype tests** (from arbloar/cas-tree): structural sharing
   (unchanged segments/pages keep their CIDs across updates), laziness
   (serving a slot loads only the spine + one segment; instrumented
   blockstore), old-root orphaning.
4. **Property tests**: random monotonic ref batches -> every inserted (slot,
   vh) resolvable, every uncovered slot 503, every covered-absent 404;
   seal/truncate/re-apply roundtrips; directory depth growth at exact
   capacity boundaries.
5. **Pin/GC tests**: sliding window + shared blob across two heads; run GC;
   assert exactly the policy-implied blocks survive. Deterministically pause
   application Get/Put/Has against candidate deletion in both linearization
   orders; publish refs, a window-expanding truncate, an adoption, and a Resume
   after T0; assert every resulting live block is in M union T. Exercise
   A-to-B-to-GC-to-A marker reset and application AllKeys lifecycle exclusion.
   Inject enumeration
   failure and cancellation, assert fail-safe epoch cleanup, and verify scrub
   detects CID corruption without deleting or refetching it.
6. **Idempotency/crash tests**: replay every ingest call; kill the daemon
   between block writes and root swap; assert old root intact and orphans
   collected.
7. **End-to-end**: fake beacon server + both indexers + bloard; sync a
   synthetic chain; then a second bloard bootstraps from the first over HTTP.
8. **Follower conformance**: a follower tracks a live writer over bitswap
   (adopting roots, pin-syncing, serving on-demand fetches) and passes test
   1 itself; no-regression rejection of stale/rolled-back publication docs;
   `verify: full` quarantines a corrupted head.

## 14. Package layout

```
bloar/
  cmd/bloard/            # archive daemon main
  cmd/bloar-index/       # indexer main (subcommands: beacon, chain)
  store/                 # on-disk state: flatfs blockstore + pebble KV (sec 6)
  core/                  # Pointer[T] (cid/loaded/dirty), node store iface
  schema/                # DAG-CBOR types + canonical encode/decode + CIDs
  archive/               # head engine: apply_refs, seal, dir ops, truncate,
                         #   read path (section 4/5)
  catalog/               # vh -> CID KV, pin ledger
  pinning/               # policy -> desired pin set, reconciler, GC
  metrics/               # prometheus registry + health/ready (7.4)
  server/                # HTTP handlers (7.x), caching, auth
  ingest/                # put-blobs pipeline: KZG verify, store, catalog
  p2p/                   # libp2p host, bitswap server/client, IPNS (11.2, 8.1)
  follow/                # follower protocol (11.3, 11.4)
  index/beacon/          # 10.1
  index/chain/           # 10.2 (v1 shipped this as index/arbitrum)
  conformance/           # separate module; nitro-client tests (13.1, 13.8)
```

Module path: set on first commit (e.g. `github.com/<org>/bloar`).

Key dependencies: `github.com/ipfs/boxo` (blockstore),
`github.com/ipfs/go-cid`, a canonical DAG-CBOR codec
(`github.com/ipld/go-ipld-prime` with dag-cbor, or `cbor-gen` with care),
`github.com/ethereum/go-ethereum` (types, `crypto/kzg4844`),
`github.com/ipfs/go-ds-flatfs` (block storage),
`github.com/cockroachdb/pebble` (node-local KV),
`github.com/libp2p/go-libp2p` + `boxo/bitswap` + `boxo/ipns` (distribution),
and nitro packages in indexers/conformance only.

## 15. Versioning

- Every DAG object carries `"v"`. Readers MUST reject unknown major versions.
- The Manifest (10.5) carries `"v"` like every DAG object, but the reject-unknown-
  major rule binds whatever DECODES it — the indexer validating a proposed
  upgrade, an auditor re-deriving a filtered head — not followers, which replicate
  the chain by generic link traversal (following `prev`) and never decode a
  manifest, so an unknown manifest version is not theirs to reject.
- HTTP API is versioned in the path (`/bloar/v1/`). Beacon-compatible paths
  follow the upstream beacon API, pinned to `eth/v1`.
- Publication doc carries `"v"`.

## 16. Open questions (non-blocking for v1)

- DHT content-routing announcements (provide records) for blocks, so
  followers can discover block holders beyond static peering; and
  IPNS-over-pubsub for fast publication propagation.
- Whether chain heads should also record the L1 block number per row
  (currently derivable via timestamp math; storing it would cost ~8 bytes/row
  and remove an RPC dependency for some consumers).
- Snapshot/export format (era-style flat files) for offline distribution.
