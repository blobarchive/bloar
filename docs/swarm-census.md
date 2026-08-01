# Decentralized swarm census

`bloar-swarm-inspect` takes one bounded observation of archive availability. It
discovers peers through the ordinary IPFS DHT from the configured Kubo daemon,
then asks each discovered peer for specific content-addressed blocks from an
ephemeral libp2p client on the machine running the inspector. With a local Kubo
these are one network vantage point. With a remote Kubo the report is explicitly
composite: provider discovery reflects the daemon's network position while
reachability and block challenges reflect the inspector's. The command does not
register with, submit results to, or query a Bloar-operated service.

The result is deliberately a **timestamped local lower bound**, not an exact
global replica count. DHT provider records are partial, cached, and eventually
expiring. A peer can also be temporarily unreachable from one probe vantage
while it is reachable from another. “Observed: 8” therefore means only “the
configured Kubo daemon found at least eight distinct provider IDs during this
bounded run.” It never means that the swarm contains exactly eight replicas.

## What the census proves

Each replica periodically provides a stable rendezvous CID derived from its
Bloar network and head names. It is the CID of a canonical tiny raw namespace
block. Kubo-backed replicas store and direct-pin one such block per configured
head because stock Kubo refuses to provide a CID absent from its blockstore.
The block contains only the public namespace components and exists as a stable
key for ordinary IPFS `Provide`/`FindProviders` operations while archive
generation CIDs change; serving it is not an archive-availability proof.

For every provider returned by Kubo's `routing/findprovs`, the inspector creates
an ephemeral, no-listen libp2p client and runs a client-only Bitswap exchange
with an empty local blockstore and no content router. A relayed dial necessarily
also connects the relay peer, so connection isolation alone is insufficient.
Every successful challenge must carry Bitswap provenance naming the exact
target provider, and its bytes are rehashed against the requested CID. A block
served by a relay or any other connected peer is rejected rather than credited
to the target.

The inspector classifies positive evidence as follows:

| State or lower bound | Meaning in this report |
| --- | --- |
| `observed` | A distinct provider ID appeared in the bounded DHT result. |
| `reachable` | The target-specific client connected to that provider. |
| `current` | The reachable provider served the exact current challenge CID. |
| `sampled_archive` / raw state `sampled-archive` | The current challenge and every configured historical sample succeeded without a probe error. |
| raw state `current-only` | The current challenge succeeded, but at least one historical sample did not. |
| raw state `stale` | The peer was reachable but did not serve the current challenge. |
| raw state `unreachable` | No target connection was established. |
| raw state `unprobed` | The bounded run ended before that admitted peer could be probed. |

Historical challenges are samples, not an enumeration of a multi-terabyte DAG.
A `sampled-archive` result is positive evidence that the peer served every
chosen depth sample; it is not a proof that every archive block exists and is
not labelled as a full replica count. Choose several
distinct CIDs from authenticated generations across the archive's history.
Choose the current CID from a source you already authenticate, such as the
selected head root or manifest tip shared by every replica of that head. A
replica controller's generation anchor includes its local replica ID and is
therefore not a swarm-wide challenge. The command validates CIDs and target
responses, but it does not decide which publication you trust.

## Run a local observation

The network and head must exactly match the values used by replicas when they
announce their rendezvous key. At least one historical challenge is required;
repeat `-historical` to sample more depths.

Most stock local Kubo APIs do not use bearer authentication. Opt in explicitly
to that loopback-only mode:

```sh
CURRENT_CID=bafy-current-authenticated-cid
EARLY_CID=bafy-early-authenticated-cid
MIDDLE_CID=bafy-middle-authenticated-cid

bloar-swarm-inspect \
  -net mainnet \
  -head all \
  -current "$CURRENT_CID" \
  -historical "$EARLY_CID" \
  -historical "$MIDDLE_CID" \
  -kubo-api http://127.0.0.1:5001 \
  -kubo-allow-unauthenticated \
  -format json \
  -pretty
```

For a protected Kubo reverse proxy, use a bearer-token file. Remote APIs should
use HTTPS because the Kubo RPC surface is administrative. Prefer co-locating
the inspector and Kubo for an ordinary single-vantage report; the remote form
below deliberately produces the split-vantage result described above:

```sh
bloar-swarm-inspect \
  -net mainnet \
  -head all \
  -current "$CURRENT_CID" \
  -historical "$EARLY_CID" \
  -historical "$MIDDLE_CID" \
  -kubo-api https://kubo-api.example.org \
  -kubo-bearer-token-file /run/credentials/kubo-rpc-token \
  -format json
```

`-kubo-api` accepts a canonical reverse-proxy path prefix as well as an origin.
Bearer authentication over plain non-loopback HTTP is refused unless the
operator explicitly adds `-kubo-allow-insecure-http`; use that exception only
on a trusted network. `-kubo-request-timeout` controls the preliminary version
and capability checks. The actual DHT and peer probes use their separately
bounded deadlines described below.

The inspector is pinned to stable Kubo `0.42.x` and fails closed on any other
release line or a partial API. A least-privilege read-only proxy needs only
`version`, command discovery, and `routing/findprovs` with
`--num-providers`; it does not need block, pin, repository, key, name, provide,
or swarm mutation endpoints. Run the command once after every Kubo or proxy
upgrade before relying on scheduled output.

As an alternative to `-net` and `-head`, an operator that has independently
derived the exact namespace-block CID may pass `-rendezvous CID`. Do not provide
both forms. The rendezvous CID is queried but never used as an availability
challenge.

### Prometheus output

Prometheus output contains aggregate, unlabelled gauges. It omits rendezvous and
challenge CIDs, peer IDs, multiaddresses, and peer-specific errors:

```sh
bloar-swarm-inspect \
  -net mainnet \
  -head all \
  -current "$CURRENT_CID" \
  -historical "$EARLY_CID" \
  -historical "$MIDDLE_CID" \
  -kubo-api http://127.0.0.1:5001 \
  -kubo-allow-unauthenticated \
  -format prometheus
```

The exposition includes lower bounds for observed, reachable, current, and
sampled-archive peers; completion and truncation flags; error and probe counts;
the observation timestamp; and its duration. A scheduler can write successful
output to a node-exporter textfile directory or another local collector. The
command always emits the partial report first but returns non-zero when the
bounded census is incomplete, so only publish a newly written file after a
zero exit status.

### Opt-in peer evidence

Default JSON also omits the `peers` array. Add `-raw-peers` only when individual
evidence is needed:

```sh
bloar-swarm-inspect \
  -net mainnet \
  -head all \
  -current "$CURRENT_CID" \
  -historical "$EARLY_CID" \
  -historical "$MIDDLE_CID" \
  -kubo-api http://127.0.0.1:5001 \
  -kubo-allow-unauthenticated \
  -format json \
  -raw-peers \
  -pretty
```

Raw output contains peer IDs, admitted multiaddresses, per-peer classifications
and errors, connection path, and dial/probe latency. It is network inventory:
restrict its filesystem permissions and retention, and do not publish it by
default. `-raw-peers` is intentionally unavailable with Prometheus output so a
normal scrape cannot create unbounded identity labels or disclose addresses.

## Bounds and failure semantics

One invocation has conservative defaults:

| Flag | Default | Purpose |
| --- | ---: | --- |
| `-max-providers` | `64` | Maximum distinct providers admitted and probed. |
| `-max-address-bytes` | `131072` | Aggregate provider multiaddress bytes admitted. |
| `-concurrency` | `4` | Maximum simultaneous target probes. |
| `-max-historical` | `16` | Maximum historical challenge CIDs. |
| `-timeout` | `45s` | Overall command work deadline. |
| `-discovery-timeout` | `15s` | DHT provider-discovery deadline. |
| `-probe-timeout` | `10s` | Deadline for one target peer. |
| `-max-probe-bytes` | `16777216` | Maximum aggregate verified challenge payload credited to one peer. |

The Kubo adapter applies additional finite ceilings to its NDJSON response,
each JSON item, event count, providers, and addresses. The target prober also
bounds CIDs, addresses, memory, connections, streams, and credited payload.
Bitswap itself caps one inbound message at 4 MiB, so a peer can send one bounded
message before a smaller aggregate payload limit rejects it. Flags can tighten
credited work; compiled hard maxima and deadlines prevent a census from
becoming an unbounded DHT or Bitswap crawl.

An incomplete discovery, timeout, cancellation, malformed response, or failed
peer probe remains visible in the report. Counts include only positive evidence
and are never extrapolated. A complete bounded run means that this configured
sample finished; it still does not make the lower bounds into global totals.

The raw connection `path` is equally local:

- `direct` means the strongest final connection this ephemeral observer saw did
  not use `/p2p-circuit`;
- `relay` means all final connections it saw used circuit relay; and
- `unknown` means no path was proved or the transport could not classify it.

This says nothing universal about the peer's NAT or reachability from other
networks. Dial and probe latency likewise describe only this run.

## Privacy and decentralization

The command has no phone-home endpoint and Bloar has no central peer registry.
Its remote interactions are the Kubo RPC calls, the daemon's normal DHT provider
lookup, and direct Bitswap challenges from the inspector to returned peers. A
remote Kubo operator can observe the authenticated RPC and rendezvous query;
DHT participants can observe a provider lookup; and a challenged peer can
observe the ephemeral client and requested CIDs. Keep the challenge set and run
frequency proportionate to the operational question.

Do not sum reports from different observers: the same peer commonly appears in
both. Separate, co-located inspector/Kubo vantage points instead answer a more
useful question—whether the same swarm is discoverable and useful from different
parts of the network. Do not count one split-vantage invocation as two
independent observations.

## Rollout acceptance

Hermetic tests can prove bounded discovery, target isolation, classification,
output privacy, and timeout behavior. They cannot prove public DHT propagation,
relay behavior, or Internet reachability. Before treating census metrics as an
operational signal, run the command from at least two independent public vantage
points, each with its own co-located inspector and Kubo, using the same
authenticated challenge set. Confirm that provider lookup, direct
target-specific reads, aggregate output, and expected direct/relay path
classification work from both.

That two-vantage public validation is a rollout acceptance step. It is not
established by a local or single-host test, and local results must not be
documented as if it were complete.
