# Swarm monitoring

This guide turns Bloar's private Prometheus registry into a small swarm
dashboard and a set of alerts based only on facts the local node can observe.
It deliberately keeps five layers separate:

1. local reachability;
2. rendezvous and exact-pointer discovery;
3. relay reservations and relay-service control traffic;
4. DCUtR direct-connection upgrades; and
5. Bitswap block transfer.

A green layer does not imply the next one is green. In particular, a DHT result
is an untrusted lead, a relay reservation is an introduction path, and bytes
scheduled into a Bitswap envelope are not delivery acknowledgement.

All examples assume Prometheus supplies `job` and `instance`. Preserve any
additional deployment labels when adapting aggregations. The metrics endpoint
must remain private as described in [Operations](operations.md#6-monitoring).

## Dashboard panels

### 1. Local reachability and connected peers

Show the one-hot AutoNAT state:

```promql
bloar_p2p_reachability
```

Show live peer cells without introducing PeerID cardinality:

```promql
sum by (job, instance, direction, transport) (bloar_p2p_live_peers)
```

`transport="relay"` means a limited relay connection exists. It does not mean
DCUtR upgraded it to a direct connection or that Bitswap can carry a blob over
it.

### 2. Rendezvous discovery

Show local provide freshness only when providing is active:

```promql
(time() - bloar_rendezvous_provide_last_success_timestamp_seconds)
and on (job, instance)
(bloar_rendezvous_active{operation="provide"} == 1)
```

Show round outcomes as rates:

```promql
sum by (job, instance, outcome) (
  rate(bloar_rendezvous_discovery_rounds_total[30m])
)
```

Show the most recent bounded provider sample beside the configured result cap:

```promql
bloar_rendezvous_observed_provider_samples
```

The sample gauge is the number of provider records this process consumed in
its last bounded round. It is not the number of providers in the DHT, not the
number of honest nodes, and not the number of reachable nodes. A zero may mean
an empty local query, propagation delay, timeout, or an unavailable DHT path.

The provide counter is per synthetic rendezvous key, so a round for several
configured heads can increment it several times. Its last-success timestamp
moves after any successful key write. Compare the `ok` and `error` counters to
detect a partially failing round.

### 3. Exact-current-pointer publication

In a split writer/edge deployment, select the `edge` role for these metrics.
The edge derives its complete root/manifest/document snapshot from the already
authenticated publication only after the primary IPNS write succeeds. The
private writer's zero-valued family is not the active schedule and must not be
unioned into never-established or freshness alerts.

Show which semantic pointer kinds are current:

```promql
bloar_pointer_current
```

Show freshness only for current pointer kinds. The timestamp is the oldest
success across every current CID of that kind and remains zero until all of
them have succeeded, so one successful head cannot hide another head's stale
or never-provided pointer:

```promql
(time() - bloar_pointer_provide_last_success_timestamp_seconds)
and on (job, instance, kind)
(bloar_pointer_current == 1)
```

Show attempts and retry causes:

```promql
sum by (job, instance, kind, outcome) (
  rate(bloar_pointer_provides_total[30m])
)
```

```promql
sum by (job, instance, kind, reason) (
  rate(bloar_pointer_retries_total[30m])
)
```

Show authenticated schedule installation outcomes separately from asynchronous
DHT attempts:

```promql
sum by (job, instance, outcome) (
  increase(bloar_pointer_schedule_updates_total[1h])
)
```

`ineligible` means the current CID was not present in the serving blockstore,
or a document was not in the verified retained set. `check_error` is a local
blockstore/verification check failure. `provide_error` means a DHT call was
attempted and failed. The last-success gauge resets to zero when a pointer CID
changes, so a newly selected root cannot inherit the previous root's apparent
freshness. These are still local DHT call facts, not remote propagation proof.

### 4. Relay control plane

Bloar uses the metrics tracers shipped by the pinned go-libp2p version. They are
registered on Bloar's private registry, but retain their upstream names.

For a private node using AutoRelay, show the desired and advertised relay
address counts:

```promql
libp2p_autorelay_desired_reservations
```

```promql
libp2p_autorelay_relay_addresses_count
```

Show reservation request outcomes:

```promql
sum by (job, instance, request_type, outcome) (
  rate(libp2p_autorelay_reservation_requests_outcome_total[30m])
)
```

For a public node offering circuit-v2 service, use
`libp2p_relaysvc_status`, `libp2p_relaysvc_reservations_total`,
`libp2p_relaysvc_connections_total`, and
`libp2p_relaysvc_data_transferred_bytes_total`. Those service-side metrics
describe reservations and bounded relay circuits handled by this node. They do
not prove that either endpoint completed a direct upgrade.

The upstream collectors are absent when host metrics are disabled and may be
absent when the corresponding feature is not constructed. Dashboard panels
should display that as “feature off / no series”, not as zero successful work.

### 5. DCUtR direct upgrade

Show overall hole-punch outcomes:

```promql
sum by (job, instance, outcome) (
  rate(libp2p_holepunch_outcomes_total[30m])
)
```

`outcome="success"` is the direct-connection fact. Relay reservations,
hole-punch signaling, and `bloar_p2p_live_peers{transport="relay"}` are only
control-plane evidence. The address-level upstream metric has bounded
`side`/`num_attempts`/`ipv`/`transport`/`outcome` dimensions, but the overall
outcome panel is usually the clearer operational view.

### 6. Bitswap data plane

Inbound fetch completion is represented by:

```promql
sum by (job, instance, outcome) (rate(bloar_bitswap_fetches_total[30m]))
```

and successful inbound payload by:

```promql
rate(bloar_bitswap_fetched_bytes_total[30m])
```

Outbound serving has a narrower observable seam:

```promql
sum by (job, instance, peer_class) (
  rate(bloar_bitswap_scheduled_bytes_total[30m])
)
```

Boxo calls this tracer before attempting the network write. The metric is raw
block payload scheduled into outbound envelopes; it excludes want/HAVE
metadata but is **not delivered bytes**, a receipt, or proof the remote peer
retained anything. Do not build a “serve-back succeeded” alert from it. Field
tests must correlate the receiver's successful fetch with sender-side
scheduled work when end-to-end proof is required.

## Alert examples

The intervals below assume the default 12-hour rendezvous and pointer
reprovide cadence and allow a full extra cycle plus jitter. Tune them together
with configuration; do not shorten the alerts independently of a deliberately
longer configured interval.

### Rendezvous provide stale

```promql
max by (job, instance) (
  bloar_rendezvous_active{operation="provide"}
) == 1
and on (job, instance)
(
  max by (job, instance) (
    bloar_rendezvous_provide_last_success_timestamp_seconds
  ) == 0
  or
  time() - max by (job, instance) (
    bloar_rendezvous_provide_last_success_timestamp_seconds
  ) > 93600
)
```

Use `for: 15m`. Investigate DHT bootstrap/connectivity and the
`rendezvous_provides_total{outcome="error"}` rate. This says local DHT writes
have not succeeded recently; it does not assert that remote records are absent.

### Repeated rendezvous discovery without an available candidate

```promql
sum by (job, instance) (
  increase(bloar_rendezvous_discovery_rounds_total{outcome="available"}[1h])
) == 0
and on (job, instance)
sum by (job, instance) (
  increase(bloar_rendezvous_discovery_rounds_total{outcome=~"empty|timeout"}[1h])
) >= 3
and on (job, instance)
max by (job, instance) (
  bloar_rendezvous_active{operation="discover"}
) == 1
```

This is actionable as “this node repeatedly failed to end a bounded round with
a connected candidate.” Do not word it as “the network has zero providers.”

### Current pointer not freshly provided

```promql
bloar_pointer_current == 1
and on (job, instance, kind)
(
  bloar_pointer_provide_last_success_timestamp_seconds == 0
  or
  time() - bloar_pointer_provide_last_success_timestamp_seconds > 93600
)
```

Use `for: 15m`. Split the response by retry reason: fix local retention or
verification for `ineligible`, local store failures for `check_error`, and DHT
connectivity for `provide_error`. A zero timestamp means at least one current
CID of the kind has never succeeded, not that every CID failed.

### AutoRelay reservation deficit

```promql
libp2p_autorelay_relay_addresses_count
< on (job, instance)
libp2p_autorelay_desired_reservations
```

Use `for: 10m` and only enable it for nodes configured with static AutoRelay
candidates. Check reservation-request outcomes before changing limits. A
reservation deficit affects the introduction path; it is not itself a Bitswap
failure.

### Sustained DCUtR failure

```promql
(
  sum by (job, instance) (
    increase(libp2p_holepunch_outcomes_total{outcome!="success"}[1h])
  )
  /
  sum by (job, instance) (
    increase(libp2p_holepunch_outcomes_total[1h])
  )
) > 0.8
and on (job, instance)
sum by (job, instance) (
  increase(libp2p_holepunch_outcomes_total[1h])
) >= 5
```

Use this as a warning on private nodes, not a fleet-wide page. Some NATs are
not punchable, and public nodes may have no reason to punch. The response is to
inspect NAT type, direct addresses, and relay/DCUtR field evidence—not to infer
malicious peers or increase the relay data cap.

## Evidence boundary

The strongest statement each layer supports is:

| Layer | Locally observable claim |
|---|---|
| Reachability | AutoNAT's current verdict and connected transport cells. |
| Rendezvous | Local DHT write result; bounded provider records consumed; whether a candidate connected. |
| Exact pointer | Local eligibility, DHT write result, retry cause, and age of the successful write for the current CID. |
| Relay | Reservation/circuit control-plane activity and bounded relay-service bytes. |
| DCUtR | Whether a hole-punch attempt established a direct connection. |
| Bitswap fetch | Whether this node fetched a content-addressed block and how many bytes it received. |
| Bitswap serve | Payload scheduled before send; no delivery acknowledgement is currently exposed. |

None of these metrics proves global provider cardinality, Sybil resistance,
remote retention, or archive correctness. Document signatures, exact CIDs,
manifest ancestry, replay floors, and block verification remain the correctness
boundary.
