# Versioned follow profiles

Bloar can expand a scalar `follow:` value into the network, beacon, trust, head,
and retention fields of a canonical follower config. Expansion happens on the
YAML syntax tree and the result then passes through the same strict
`KnownFields` decoder and semantic validation as a hand-written mapping.

Follow profiles are local policy, not a network registry. The binary contains
an immutable follow-profile bundle and a follower/replica operator may add one
explicit local bundle. There is no remote lookup or automatic follow-profile
update path.

## Permissionless publication, curated built-ins

Running and publishing a writer is permissionless. Any operator may derive
heads, publish a signed document through IPNS, put DNSLink under a domain they
control, and distribute a local follow profile that pins their signer. Any
follower/replica operator may select that profile. Neither side needs a
`blobarchive.net` name, an entry in this repository, or project approval.

This is particularly useful for an L2 that the built-in catalog does not yet
cover: its owner, infrastructure providers, or community operators can launch
writers immediately instead of waiting for the project to add a default.

What the project curates is narrower:

- DNS records under `blobarchive.net`;
- follow profiles embedded in released Bloar binaries; and
- which writers the project presents as its reviewed defaults.

Inclusion there is a namespace and distribution decision, not admission to the
protocol. A writer published under another domain is equally usable once a
follower operator deliberately selects and pins it.

“Follow profile” is the full term throughout this guide. It is unrelated to a
Docker Compose `profiles:` activation guard: selecting a Bloar follow profile
never starts another service and no command in this guide uses
`docker compose --profile`.

Follow-profile names are **opaque lookup keys**. Never split a name on hyphens
or infer its chain, network, finality, head, or writer. Only the selected record
defines those fields.

## Selecting a follow profile

A scalar `follow` is the selector. `profile.file` is resolved relative to the
daemon config. The rest is still a minimal complete daemon config: follow
profiles do not supply store paths, credentials, listener policy, private keys,
or resource budgets.

```yaml
profile:
  file: follow-profiles.yaml
follow: example-chain

store: {path: /var/lib/bloar}
server: {auth_token_file: "${CREDENTIALS_DIRECTORY}/token"}
p2p: {}
```

Every follow profile has a canonical versioned selector such as
`example-chain@v1`. An unversioned spelling such as `example-chain` is an
explicit alias and stays attached to that version; adding v2 does not silently
move it. Names and aliases cannot collide across built-in and local bundles, so
local follow profiles never override built-ins.

Inspect and dry-run a config without opening the store, reading the bearer
token, reading a signing key, resolving DNS, or joining libp2p:

```console
bloard config-inspect -config /etc/bloar/bloard.yaml
```

The output contains the selected name, schema, version, SHA-256 content digest,
source/provenance, and the complete validated effective config. It contains
secret *file references* but never reads or prints secret contents.

## Built-in production follow profiles

The initial production catalog contains one immutable, full-retention follow
profile per finalized head on writer `a`, plus a derived live view for each:

| Selector | DNSLink authority | Followed data | Retention |
|---|---|---|---|
| `ethereum-mainnet-all-a` | `ethereum-mainnet-all-a.blobarchive.net` | `all` | full |
| `ethereum-mainnet-arb1-a` | `ethereum-mainnet-arb1-a.blobarchive.net` | `arbitrum-one` | full |
| `ethereum-mainnet-robinhood-a` | `ethereum-mainnet-robinhood-a.blobarchive.net` | `robinhood` | full |
| `ethereum-mainnet-base-a` | `ethereum-mainnet-base-a.blobarchive.net` | `base` | full |
| `ethereum-mainnet-all-live-a` | `ethereum-mainnet-all-live-a.blobarchive.net` | `all` + mutable `unfinalized` → local `live` view | full |
| `ethereum-mainnet-arb1-live-a` | `ethereum-mainnet-arb1-live-a.blobarchive.net` | `arbitrum-one` + mutable `unfinalized` → local `live` view | full |
| `ethereum-mainnet-robinhood-live-a` | `ethereum-mainnet-robinhood-live-a.blobarchive.net` | `robinhood` + mutable `unfinalized` → local `live` view | full |
| `ethereum-mainnet-base-live-a` | `ethereum-mainnet-base-live-a.blobarchive.net` | `base` + mutable `unfinalized` → local `live` view | full |

Each shorthand is an explicit v1 alias; the canonical spelling appends `@v1`.
There is deliberately no bare alias that can move between writers. A second
writer included in this built-in catalog gets a distinct authority and
follow-profile key such as a `-b` suffix. Changing a signer requires the
follower operator to select a reviewed new follow-profile version; changing a
DNSLink record cannot bypass the independently pinned signer.

All eight records pin Ed25519 public key
`6698f6c8767529ffb725ce5201a86602106cc87ed7c9129a649428ca0ea6d7b5`.
They contain no public HTTP URL: discovery is DNSLink → IPNS → exact signed
publication document, and blob transfer is libp2p/Bitswap. Each also fixes
`verify: full`, so every served blob is checked by recomputing its KZG
commitment rather than relying only on CID/multihash verification.

### Live-view freshness

`-live` means that the selected publication includes the bounded mutable head;
it does not mean that every Ethereum slot is streamed to followers immediately.
Writers publish authenticated generations in batches, and an embedded follower
polls for the next generation every 60 seconds by default. With the current
public publication path, normal steady-state end-to-end lag is roughly one to
two minutes. DHT conditions and bounded retries can occasionally extend that
delay to several minutes. These are healthy-path expectations, not a ceiling.
A publication outage can keep returning a valid but frozen generation for much
longer, so alert on the age of the last accepted generation rather than on
request errors alone.

This is the time to discover and adopt a new generation, not an initial-sync
estimate or a guarantee that every block in a newly selected generation is
already retained locally. Initial replication and catch-up after downtime are
separate and can take much longer. Lowering a follower's poll interval cannot
make it observe a generation before the writer has successfully published it.

The v1 content digests, embedded and verified at startup, are:

- `ethereum-mainnet-all-a@v1`:
  `sha256:9c175ccedc95e9a0e910a128eadd4c5bd767ce980e1bbe495d65b76ad83d978e`
- `ethereum-mainnet-arb1-a@v1`:
  `sha256:5834dba26a6c6159c8393dd494f926393f5db9d4e3aa44f0d9c596556a285fa5`
- `ethereum-mainnet-robinhood-a@v1`:
  `sha256:ff45e33a71c9b900a02a3380c6c3cbf80750f96500eb7dedc42971d2c165260e`
- `ethereum-mainnet-base-a@v1`:
  `sha256:c0d4cdf272542a027ed054f6c12edd445d4ce26d69aa0640043712410ef51958`
- `ethereum-mainnet-arb1-live-a@v1`:
  `sha256:4b16e6ed186544e9ddd4136cebe9b93d42a36ca4280fd9fcf205610476be4d43`
- `ethereum-mainnet-robinhood-live-a@v1`:
  `sha256:dfc42695a695567b9023b845c31ff8da072104a4ecc3dce8e4db9b02a7249d43`
- `ethereum-mainnet-base-live-a@v1`:
  `sha256:28bcc485b823fe9144f55980960da43fbad704de1e64d50fdf001cccedca081f`
- `ethereum-mainnet-all-live-a@v1`:
  `sha256:3c2e5bbcf887f674c0d28cb4fcffcffa72156bd8a3437a3baa85dead3239a59f`

Unmarked names mean finalized data. The `-live` records do not pretend that
`live` is another signed publication head. Each follows its independently
published finalized head and the shared mutable `unfinalized` head. The mutable
head always pins the publication's authenticated global `handoff_head: all` and
`max_window_slots: 128` contract.

`all-live-a` uses that handoff directly. The Arbitrum One, Robinhood, and Base
profiles configure exact-hash overlays: their local `live` view uses the filtered
finalized head as its retained serving frontier while the mutable proof remains
bound to `all`. `require_versioned_hashes: true` prevents the shared provisional
tip from becoming an enumeration surface, and whole-document admission refuses
a gap between the retained filtered frontier and the mutable window. Sharing
the small mutable set therefore does not make chain-specific reads return
unrelated blobs: the beacon request names the versioned hashes the L2 needs.
The selected physical heads must come from one authenticated publication
document, and a complete mutable generation is fully pinned before it becomes
selectable.

## Local bundle schema

This `.invalid` example is deliberately hermetic and is not a production trust
target:

```yaml
schema: bloar.follow-profile-bundle/v1
profiles:
  - schema: bloar.follow-profile/v1
    name: example-chain
    version: 1
    aliases: [example-chain]
    provenance:
      source: operator-reviewed local policy
      revision: change-123
    network:
      name: mainnet
      beacon:
        genesis_time: 1606824023
        seconds_per_slot: 12
        genesis_validators_root: "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
        genesis_fork_version: "0x00000000"
    trust:
      mode: dnslink-delegated
      dnslink: writer.example.invalid
      url: https://writer.example.invalid/heads
    verify: cid
    heads:
      example-chain:
        pin: {mode: window, duration: 720h}
```

Trust mode and retention are separate and mandatory:

- `dnslink-delegated` authenticates signer rotation through DNSLink → IPNS →
  exact document CID and forbids `pubkey` in the profile.
- `dnslink+signer-pin` requires a 32-byte hex Ed25519 `pubkey`; a signer change
  is refused even when DNSLink/IPNS authenticates it.
- `verify` is `cid` or `full`. Omitting it preserves the `cid` daemon default;
  production built-ins state `full` explicitly.
- Every followed head independently declares `pin: full`, `pin: none`, or a
  positive bounded `pin: {mode: window, duration: ...}`. `full` and `none`
  cannot carry a duration.

The optional follow-profile `digest` is checked against Bloar's canonical SHA-256
digest. Omitting it does not omit digest protection: Bloar always computes,
inspects, logs, and persists the digest.

## Conflicts, overrides, and upgrades

`net` and `beacon` cannot appear both at the config root and in a selected
follow profile. An intentional exception goes under `profile.overrides`; it may
replace only an exact scalar leaf that the follow profile already supplies. It
cannot add fields or replace an entire mapping/list:

```yaml
profile:
  file: follow-profiles.yaml
  overrides:
    follow:
      heads:
        example-chain:
          pin:
            duration: 168h
follow: example-chain@v1
```

Bloar persists `{name, schema, version, digest}` in the node KV. If
follow-profile content changes while those identity fields do not, startup
fails. After reviewing `config-inspect`, acknowledge exactly the new digest to
advance the record:

```yaml
profile:
  file: follow-profiles.yaml
  acknowledge_digest: sha256:<64-lower-case-hex-digits>
```

The acknowledgement must equal the selected content digest, so a stale value
cannot approve a later change. A new version is separately selectable and does
not need an acknowledgement: its versioned selector is the explicit policy
change.

Follow-profile-bearing config and bundles reject duplicate keys, YAML merge
keys, aliases/anchors, unknown schema fields, malformed network constants,
invalid trust combinations, URL userinfo/query/fragment (including query-string
credentials that inspect could expose), missing heads, and unsafe retention
policies before the daemon opens its store or listeners.

## Production authority operations

Treat the Cloudflare zone, writer IPNS key, publication signing key, and
follow-profile bundle as four distinct controls:

- DNSLink is a one-hop TXT record whose value is `/ipns/<name>`. Its TTL is a
  cache lifetime, not an expiry or cleanup mechanism.
- The IPNS record authenticates an exact publication-document CID.
- The document carries its own Ed25519 signature. Built-ins pin that signer, so
  DNS or IPNS compromise can cause unavailability or rollback attempts but
  cannot authorize a new signer.
- The follower persists replay, revision, manifest, root, and profile-digest
  floors. Restoring old DNS/IPNS content does not reset those floors.

Routine IPNS republishing and a DNS target change are allowed only when they
retain the same pinned signer and do not regress durable follower floors. For a
signer rotation, publish a new immutable follow-profile version with the new
key, review its `config-inspect` output and digest, then select that version
explicitly. Do not repoint an existing `-a` authority at writer `b`.

Rollback restores the last reviewed DNSLink target and writer publication state
without changing the pinned signer. Removal is explicit: stop advertising the
follow profile in release/site material first, preserve the DNSLink TXT during
the announced migration window, and delete it only after no supported config
selects it. Expiring an API token never deletes a DNS record.
