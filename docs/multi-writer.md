# Independent writers

This guide describes how to operate two or more independent Bloar writers for
one logical archive. The goal is writer-outage tolerance: a follower can keep
advancing from another compatible writer when one writer or its upstream is
unavailable.

This is not leader election or Byzantine consensus. Writers do not share a
lease, coordinator, private key, mutable store, or revision counter, and a
majority does not decide which claim is true. Every writer remains a separate
authority explicitly authorized by each follower. The follower admits only a
claim that is provably compatible with the archive it already serves; on a
conflict or an incomplete proof it keeps serving its last good generation.

The initial multi-writer contract applies only to `finalized-monotonic` heads.
An `unfinalized-mutable` head has exactly one authority. See
[specification 11.3.1](spec.md#1131-logical-archive-identity-and-finalized-multi-writer-order)
for the normative comparison rules.

Bounded multi-source follower configuration, concurrent resolution, durable
source-local replay floors, and finalized-claim arbitration are implemented.
The operator explicitly acknowledges every source-roster generation; a
source-set follower never falls back to the singular authority fields.

## 1. Topology

```text
 independent chain inputs                 independent chain inputs
            |                                        |
            v                                        v
  Writer A, key A, store A                 Writer B, key B, store B
  URL/IPNS/PeerID A                        URL/IPNS/PeerID B
            |                                        |
            +---------------+------------------------+
                            |
                  locally authorized sources
                            |
                         Follower
                 last-good generation on doubt
```

All writers use the same logical archive definition, but each writer is an
independent physical copy. Each physical head still has exactly one mutation
owner: only the local writer mutates its selector and generation state. There
is no active/passive leader and no shared write path.

Do not hide the writers behind one round-robin URL or one shared IPNS name.
Followers need stable, source-specific identities so they can authenticate a
claim, retain its replay floors and provenance, and report which source is
unhealthy or conflicting. A common CDN may cache immutable blocks, but it must
not erase the identity of the publication authority.

### Shared logical inputs

Every writer in the logical archive shares:

- one public, stable `publish.archive_id`;
- the network and head names;
- `finalized-monotonic` head kind and immutable head parameters
  (`origin_slot`, `seg_bits`, and `fanout_bits`);
- the intended finalized chain history;
- for filtered heads, the exact ordered manifest history from genesis through
  the current tip; and
- the same filter meaning and source-range schedule for each filtered head.

An operational batch size, polling cadence, cache size, or worker count does
not define logical identity and may differ. Keep content-defining configuration
identical and version controlled.

### Separate failure domains

Every writer has its own:

- host, storage, store/KV backup, and process supervision;
- publication signing key and signer-local revision state;
- libp2p private key, PeerID, IPNS name, and IPNS sequence state;
- HTTPS endpoint and any DNS name used to reach it;
- indexers and mutation credential;
- beacon-chain input and, for filtered heads, parent-chain RPC input; and
- monitoring and administrative control path.

Never copy a live writer's private signing key, libp2p key, mutable KV, IPNS
sequence, or store directory into another concurrently running writer. Never
add a lease or coordinator to serialize independent writers. Content-addressed
blocks may be copied or fetched, but mutable authority state may not be shared.

## 2. Minimum useful diversity

Two writers are the minimum useful deployment: one may fail while the other
continues. Use three when one planned maintenance event plus one independent
failure must still leave redundant service.

Merely running two processes is not diversity. Put writers in separate power,
storage, network, and administrative failure domains. Their chain inputs should
not terminate at the same node, reverse proxy, provider account, or other single
backend. In particular:

- an ALL-head writer should obtain finalized existence from an independent
  beacon authority;
- a filtered-head writer should independently obtain both beacon blob bytes and
  the parent-chain history that determines which batches belong to the head;
- each writer should retain the complete archive (`pin: {mode: full}`); and
- each writer should have at least one directly testable publication path and
  one reachable libp2p path.

Public DHT/rendezvous discovery can improve connectivity, but it does not
replace a known path to each writer. In a private swarm, configure an explicit
peer mesh. Writers may peer with one another and with replicas to make immutable
blocks easier to fetch; a peer connection does not authorize publication claims
and must not permit cross-writes.

## 3. Stable identity and source inventory

Generate the logical archive ID once:

```sh
openssl rand -hex 32
```

Configure that exact 64-character lowercase value as `publish.archive_id` on
every writer. It is a public namespace, not a credential or a key roster. It
does not rotate when a signer, URL, IPNS name, PeerID, host, or authorized source
changes. Changing it declares a different logical archive.

Give every writer a new publication signing key and a new libp2p identity.
Maintain a version-controlled public inventory with one stable operator source
ID per authority. At minimum record:

- source ID and lifecycle state;
- publication public key;
- HTTPS URL and IPNS or DNSLink name;
- PeerID and reachable multiaddresses;
- archive ID, network, and enabled heads; and
- activation and retirement metadata.

The source ID is local policy and provenance, not a field in the signed wire
document. Never recycle it for a replacement authority. If an old authority
returns, use its original source ID and retained floors; if the authority was
rebuilt with a new key, give it a new source ID.

Keep retired source records and the durable replay floors, provenance, conflict
evidence, and last trusted frontier that represent their admitted history.
Archive signed documents when they are collected as operator evidence; this
does not require the follower to be an append-only archive of every document it
has polled. Removing a source from the active authorization set must not erase
that durable state. Wall-clock timestamps are useful for logs and certificates,
but they never order claims. Signer-local revision numbers and IPNS sequence
numbers are also comparable only within that source, not across writers.

### Follower source-set configuration

Each source names only the heads it may contribute. Multiple sources may name a
`finalized-monotonic` head. Exactly one source must name each
`unfinalized-mutable` head.

```yaml
follow:
  archive_id: "<64 lowercase hex characters>"
  sources:
    writer-a:
      url: https://writer-a.example.org
      ipns: k51qzi5uqu5d...
      pubkey: "<64 lowercase hex characters>"
      heads: [all, unfinalized]
    writer-b:
      url: https://writer-b.example.org
      ipns: k51qzi5uqu5d...
      pubkey: "<64 lowercase hex characters>"
      heads: [all]
  source_set:
    revision: 1
    acknowledge_digest: "sha256:<canonical roster digest>"
  heads:
    all:
      pin: {mode: full}
    unfinalized:
      kind: unfinalized-mutable
      handoff_head: all
      max_window_slots: 64
      pin: {mode: full}
```

`url` may accompany either one name channel; `ipns` and `dnslink` are mutually
exclusive. Public keys are always pinned per source. Source IDs, normalized
channels, keys, allowed heads, network, and archive ID are covered by the
acknowledgement digest. Run `bloard config-inspect -config <path>` with the new
roster: on a missing or stale digest it prints the exact expected value. Review
the normalized roster, copy that value, and increment `source_set.revision` for
every later roster change. Reusing the digest under a new revision is harmless;
changing the roster without a higher revision is refused by durable state.

The revision is an irreversible local authorization floor, not a configuration
version that can be decremented during rollback. Bloar commits it synchronously
when the follower is constructed, before polling starts. If a later daemon
startup step fails after that commit, recover by **rolling forward**: restore the
previous roster in configuration, give that roster a revision greater than the
committed floor, recompute and acknowledge its digest, then start again. Do not
try to restore an older revision. The failed generation authorized only its
declared sources; it did not admit a publication document or change a served
head.

When converting an existing singular follower, set
`migrate_legacy_source: writer-a` for the source whose key and name authority
match the old configuration. Migration copies unambiguous replay state; it does
not invent source provenance for old checkpoints. Those heads remain unserved
after restart until a fresh signed claim proves content continuity and writes a
source-attributed checkpoint. Keep the migration field stable in configuration;
it is excluded from the roster digest and does not repeat or delete old state.

## 4. Bootstrap an independent writer

There are two safe bootstrap paths. The first gives the strongest independence;
the second is faster but inherits some trust from the seed.

### 4.1 Rebuild from independent chain inputs

1. Provision a fresh store, signing key, libp2p identity, endpoint, and
   upstreams in a separate failure domain.
2. Configure the same archive ID and immutable logical-head parameters.
3. For every filtered head, publish the recorded genesis manifest and require
   its CID to match. Run the indexer under that exact schedule to the first
   recorded transition boundary. At each transition, use
   `bloar-index publish-manifest` to apply the exact successor against the
   generation at that boundary, require the printed CID to match, then continue
   under the successor schedule. Manifest upgrades are bound to the current
   head generation, so replaying every successor before rebuilding its boundary
   is not equivalent. The ALL head has no filter manifest.
4. Run this interleaved rebuild from the independent beacon and parent-chain
   inputs until the desired finalized boundary.
5. At that fixed boundary, compare each head with an established writer
   using the deterministic audit in [operations 7.4](operations.md#74-deterministic-replication-and-auditing-an-archive-you-dont-run).
   Equal coverage must produce the same root CID. At unequal coverage, the
   higher root must project exactly to the lower root.

Do not authorize the writer merely because it has caught up. Authorization
follows the proof gate in section 6.

### 4.2 Seed immutable data, then prove it independently

A full follower promoted to writer is the safest way to copy an exact selected
root, manifest tip, and DAG without cloning writer authority state. Give the
promoted copy a new writer key and publication state, retain the logical archive
ID, and switch it to genuinely independent upstreams before treating it as
failure-domain diversity. The promotion procedure is in
[operations 7.2](operations.md#72-writer-failover-promotion).

Mirror mode can reproduce a root, but it copies the source's coverage decisions
and does **not** import or independently validate its manifest history. If it is
used as a data seed, replay the exact manifest chain separately and complete an
independent anchored audit. Root equality with a mirror proves faithful copying,
not that the source omitted nothing. See
[deterministic replication and auditing](operations.md#74-deterministic-replication-and-auditing-an-archive-you-dont-run).

Copying immutable blocks from an existing archive is safe. Cloning its live KV,
private keys, or publication counters into a concurrently running writer is not.

## 5. Preserve exact manifest history

A filtered head's manifest CID commits to its schema version, head name, full
ordered source list, and predecessor CID. The same filter described in a
different order or linked to a different predecessor is a different history.

Keep every versioned source schedule and its expected manifest CID in source
control. For an upgrade:

1. Confirm every writer starts from the same manifest tip.
2. Run the same successor preflight against every writer's current generation.
3. Publish the successor on each writer and require the returned CID to be
   identical everywhere before the indexers use the new schedule.
4. If one writer rejects the transition or produces a different CID, remove it
   from advancement, investigate, and repair its exact chain. Never mint a new
   genesis manifest or skip a predecessor to make it catch up.

Writers may temporarily have different finalized coverage. That is safe when
the lower root is an exact prefix of the higher root and their manifest tips are
equal or ancestor/descendant in the same direction. If one writer has greater
coverage while another has the descendant manifest, the complete claims are
incomparable; followers must hold their last good generation until the
directions agree.

## 6. Proof gate before authorization

Before adding a writer to a follower's active source set, verify all of the
following:

1. Its signed version-3 document authenticates under the expected public key.
2. The archive ID, network, head name, kind, and immutable parameters exactly
   match the logical archive inventory.
3. At a fixed coverage boundary, its root is equal to or an exact projected
   extension of an already trusted writer's root.
4. Its manifest tip is equal to or a valid descendant of the recorded history,
   and root and manifest ordering point in compatible directions.
5. The follower can fetch the signed document and required proof blocks through
   that source's own publication and peer paths.
6. The new writer has advanced from its independent upstreams after bootstrap,
   proving it is not still coupled to the seed.

Do not substitute matching timestamps, a larger revision, a higher IPNS
sequence, source-list order, arrival order, or a majority vote for these checks.
A timeout or missing proof block means **not yet proven**, not conflict. A
different root at equal coverage, a failed prefix projection, or divergent
manifest ancestry is a conflict. In either case the safe serving state is the
last durably admitted generation.

## 7. Lifecycle procedures

Source authorization is follower-local policy. Apply each change to every
follower that should recognize the writer, while preserving the source's durable
history.

### Add a writer

1. Provision it with isolated state, keys, endpoints, and upstreams.
2. Bootstrap it by section 4 and preserve exact manifest continuity by section
   5.
3. Add its stable source ID, public key, publication paths, and peer paths while
   the existing writers remain authorized.
4. Complete the proof gate in section 6 and observe successful document and
   block retrieval through the new source.
5. Only then count it toward redundancy or schedule maintenance on another
   writer.

### Remove a writer

1. Prove that at least one remaining source is reachable and has an equivalent
   or dominating compatible claim for every affected head.
2. Remove the writer's key and endpoints from the active authorization set.
3. Stop its indexers and publication only after followers no longer depend on
   it.
4. Retain its source record, replay floors, provenance, conflict evidence, and
   last trusted frontier indefinitely. Preserve any signed documents already
   collected as operator evidence. Mark the source retired; do not delete or
   reuse its ID.

If the same intact authority returns, re-enable its original source record and
floors. A rebuilt replacement with a new key follows the add procedure under a
new source ID.

### Rotate a publication signing key

Treat a planned signing-key rotation as source replacement:

1. Create a new key and source ID on an independent writer state, with the same
   stable archive ID.
2. Add it while the old source remains authorized.
3. Prove an equivalent or dominating claim and successful fetches.
4. Remove and retire the old source, retaining all of its history.

Do not copy only the old private key to a new revision store. A key without its
signer-local revision history can replay or equivocate. The archive ID does not
rotate with the key.

### Rotate a PeerID, IPNS name, or endpoint

Transport identity is distinct from publication authority. Add the new
PeerID/name/URL beside the old path, verify that it resolves to the exact signed
document and that blocks are fetchable, then remove the old path. Retain the old
name's sequence floor so a later return cannot replay an older record.

If one key was improperly reused for both publication signing and libp2p, treat
loss of that key as a publication-key compromise, not a transport-only rotation.

### Respond to a compromised writer

1. Isolate the writer and immediately remove its key from active authorization.
   Do not wait for a replacement or a quorum.
2. Preserve any collected signed documents, IPNS records, logs, source floors,
   conflict evidence, and the last trusted frontier for investigation.
3. Compare the last accepted generations with independent writers and external
   chain history to determine the last trusted boundary.
4. Remember that deauthorization prevents future admission; it does not undo a
   compromised generation already adopted. Explicitly truncate, rebuild, or
   restore followers if the bad generation crossed the trusted boundary.
5. Build the replacement with a new signing key, source ID, store, and transport
   identity, but the same stable archive ID. Add and prove it by the normal
   procedure.

Never delete the compromised source's history to make an alert disappear. That
history is what prevents replay and makes a later forensic comparison possible.

## 8. Failure expectations

| Event | Expected follower behavior |
|---|---|
| One writer is unavailable | Advance from another compatible, proven writer. |
| A proof block or channel is temporarily unavailable | Keep the last good generation, retry, and report the source unhealthy. |
| Authorized writers conflict | Keep the last good generation and require operator investigation; do not vote. |
| Claims are incomparable | Keep the last good generation until coverage and manifest order become comparable. |
| A source is removed | Stop admitting new claims from it, while retaining its floors and history. |

The success criterion is not that all writers publish at the same instant. It is
that any independently rebuilt writer produces the same content at the same
finalized boundary, that a follower can prove append-only continuity across
their claims, and that loss of one failure domain does not stop compatible
archive progress.

### Observe source continuity locally

A source-set follower exports its own bounded observations; it does not ask
writers to report to a central service and does not require Prometheus to scrape
the writers. The source and head labels are drawn only from local configuration.
Publication URLs, keys, CIDs, revisions, PeerIDs, and error strings never become
labels.

Use the five source-set series together:

- `bloar_follow_source_available{source}` is 1 only when that source's latest
  serialized poll produced a document which survived authentication, archive
  binding, replay floors, mutable-handoff validation, and quarantine checks. It
  is publication-plane health, not TCP reachability or Bitswap delivery.
- `bloar_follow_source_last_success_timestamp_seconds{source}` records local
  receipt time for the last such document. It is not the writer-controlled
  `updated_at` value.
- `bloar_follow_source_head_covered{head,source}` and
  `bloar_follow_source_head_synced_to{head,source}` describe the latest
  successfully observed claim for an authorized cell. An uncovered claim has
  no `synced_to` series because slot zero is valid. During an outage the last
  claim remains visible; pair it with availability or success age rather than
  mistaking it for a current observation.
- `bloar_follow_source_selected{head,source}` is one-hot durable last-good
  selection provenance. It changes only after a selected checkpoint or
  withdrawal crosses the serving transaction's durability barrier, and remains
  selected while all writers are down or the head is quarantined. Pair it with
  `bloar_follow_head_ready{head}` for current serviceability. It does not
  identify which peer supplied individual blocks.

Alert separately on one source disappearing, all sources disappearing while a
last-good head remains ready, all sources disappearing with no serviceable
checkpoint, an active conflict latch, sustained incomparable claims, and a
follower falling behind the best currently available covered claim. Also alert
on the follower scrape target itself: absent telemetry is not healthy telemetry.

Bitswap fetch counters and libp2p peer metrics are the separate data-plane view.
A source may publish an admissible claim while its advertised peer cannot serve
the closure, or immutable blocks may arrive from a peer which has no publication
authority. Neither observation substitutes for the other.

## 9. Investigating and clearing a conflict latch

A follower durably latches a finalized head only after it has closed a
cryptographic contradiction between authorized sources: equal coverage with
different roots, a failed prefix projection, or two readable manifest histories
which are not descendants of one another. A missing proof, timeout, malformed
publication, or temporarily incomparable coverage and manifest order is not a
latch. Those transient conditions keep the last good generation and are retried
without operator intervention.

An active latch freezes advancement of that physical head while the follower
continues serving its last durably admitted generation. Other heads and source
replay floors continue to advance. Convergence, restart, and removing a source
from the active roster do not clear the evidence. On a follower with no last
good generation, the latched head remains unavailable.

Prometheus exposes `bloar_follow_conflict_active{head}` for the durable latch,
`bloar_follow_conflicts_total{head,source}` for newly created evidence, plus
`bloar_follow_incomparable_active{head}` and
`bloar_follow_incomparable_total{head}` for retryable partial orders. Alert on
the active conflict gauge; the incomparable gauge is useful for
diagnosing writers which remain out of step without asserting a fork. Head and
source labels are materialized only from local configuration. Evidence IDs,
CIDs, revisions, peers, and transport data never become metric labels.

Inspect latches with the daemon stopped; the command opens the store
exclusively so its report and any later clear cannot race a poll:

```sh
bloard conflicts status -config /path/to/bloard.yaml
bloard conflicts status -config /path/to/bloard.yaml -head all
bloard conflicts status -config /path/to/bloard.yaml -json
```

The report is ordered by head and includes the evidence ID, occurrence
sequence, closed reason, source-set revision and digest, deterministic evidence
pair, and count of conflicting pairs. Each endpoint is marked as an
authenticated `source` claim observed when the latch was created or a
`durable_checkpoint` (including its checkpoint format version), so a fresh claim
conflicting with the follower's own last-good state is not misrepresented as a
simultaneous two-source observation. Preserve the report before changing policy
or repairing a writer. The document digest is the correlation key for a signed
publication body captured elsewhere; preserve that body too when it is still
available. The latch intentionally retains only bounded evidence summaries, and
documents touching a latched head do not enter the ordinary admitted-document
callback.

Investigate the writers and independently establish which history is correct.
Quarantine or repair the faulty authority, then update the follower's source
roster if necessary using the normal monotonic source-set procedure. Clearing a
latch does not choose a root, roll state back, erase source floors, or authorize
a source; it only permits that head to be reconsidered on a later poll.

Clear exactly the evidence you reviewed:

```sh
bloard conflicts clear -config /path/to/bloard.yaml \
  -head all -evidence sha256:<64-lowercase-hex-characters>
```

Both the head and full evidence ID must match the currently active latch. A
missing, stale, or mistyped ID fails without changing the store. A successful
clear is retained in bounded operator history, and a later contradiction gets a
new sequence and evidence ID. Keep the daemon stopped until the command exits,
then start it and watch the conflict and source-health metrics while the next
poll proves a compatible generation.

The first durable latch upgrades the store's source-set capability marker.
Older binaries which do not understand conflict latches will refuse that store
rather than start while silently ignoring the safety state. Do not bypass that
downgrade protection.

An embedded source-set follower also refreshes its block-exchange discovery
session at each source-poll boundary, before resolution and again after dialing
authenticated publication multiaddrs. Existing connections are not replayed or
dropped, and requests already in flight finish on their current session. The
next network miss starts a fresh session so it can discover a surviving writer
instead of retaining affinity to a writer which disappeared. This is part of
failover correctness, not an operator tuning switch. A writer disappearing in
the middle of one multi-block walk can still fail that in-flight poll; the next
poll boundary refreshes discovery and retries through the surviving writer.
