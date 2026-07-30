# bloar: Operations

How to run bloard: what is on disk, which bytes are precious, what to watch, and
what to do when something has gone wrong.

This is the operator's manual. [spec.md](spec.md) is normative and this is not;
where they disagree, the spec is right and this is a bug. Section numbers below
refer to it.

Deployment artifacts live in [`deploy/`](../deploy): a `Dockerfile`, systemd
units, and example configs for each role. The examples are checked against the
real config loader by a test, so they parse; the values in them are realistic
but they are not a substitute for reading spec 12.

---

## 1. Roles

A node's role is per-head and comes from its config (spec 11.1). There is no
`role:` key.

| Role | Config | What it does |
|---|---|---|
| **Writer** | `heads:` block | Runs the mutation engine, accepts ingest, and publishes one physical copy. Each physical head has one mutation writer; a v3 finalized logical archive may have several [independent writers](multi-writer.md). |
| **Follower** | `follow:` block | Replicates a writer's heads, serves them identically, writes none of them. |
| **Both** | both blocks | Writes some heads, follows others. Supported; what it may not do is both to the same head. |

A follower may use a canonical mapping or the local, immutable scalar shorthand
described in [Versioned follow profiles](follow-profiles.md). A profile supplies
network/trust/head/retention policy only; store paths, credentials, listener
posture, and resource budgets remain explicit node config.

The **indexers** (`bloar-index`) are not a role. They are HTTP clients of a
writer's bloar API and hold no state of their own: their whole progress state is
the archive's `synced_to`, read back over HTTP (spec 10). Restart them, move
them, run one twice by mistake -- the archive notices nothing worse than
duplicated work, because spec 5.1's replay path is idempotent.

They are also **crash-only**: an indexer that exhausts its retry budget exits
rather than looping, and being stateless it expects to be restarted, not
babysat. Run them under a restarter -- the shipped systemd unit sets
`Restart=always` -- because under a supervisor with no restart policy (a compose
file's default) a dead indexer is silent: the writer keeps serving, coverage
just stops advancing, and the first symptom is a stale `synced_to`.

One thing the chain indexer (spec 10.2) trusts and cannot check: that its
`parent_chain_rpc` answers `eth_getLogs` completely. An `inbox-events` source
selects a chain's batches by asking the node for every matching log over a
finalized range, and a provider that silently caps or truncates an over-large
result set drops those batches -- each dropped batch is then recorded as a
permanent 404 that replay and followers inherit (spec 10.4), with nothing in the
indexer to notice the short answer. Point a chain indexer at your own full node.
If you must use a third-party RPC, verify how it behaves on an over-large
`getLogs` result before trusting it with a head: a complete answer or an error is
safe, a silent truncation is not. `blob-txs` sources read full blocks and are not
exposed. Their cost is instead one full `eth_getBlockByNumber` per L1 block.
`index.block_fetch_concurrency` (default 4, max 32) bounds the worker pool and
`index.rpc_batch_size` (default 16, max 128) bounds the consecutive calls each
worker sends as one JSON-RPC batch. Fetches may complete out of order; the
indexer reduces them strictly by block number and transaction-body order, so
tuning throughput cannot change a head's rows or root. The
`bloar_index_l1_block_fetch_*` metrics separate the accelerated `batch` path
from the compatible `fallback` path used by an embedded ChainClient that does
not implement batching.

---

## 2. What is on disk

Everything is under `store.path` (default `/var/lib/bloar`):

```
/var/lib/bloar/
  blocks/          flatfs: one file per block. The archive.
  kv/              the Pebble KV (its own subdirectory, not the store root):
                   catalog, pin ledger, roots, manifest tips, IPNS sequence,
                   follower checkpoint/floors.
  p2p.key          the libp2p identity (default p2p.identity_key_file).
```

### 2.1 The blocks are precious; most of the KV rebuilds, not all of it

This decides the whole backup story, so state it precisely rather than as a
slogan: `blocks/` is irreplaceable, most of the KV is a derived cache, and a
small but load-bearing part of the KV is neither derivable nor in the DAG.

**`blocks/` is the archive.** Blob blocks are 128 KiB of EIP-4844 blob data that
may no longer exist anywhere else on Earth: beacon nodes prune them after ~18
days. Index blocks (`Head`, `DirNode`, `Segment`) are cheap to recompute only if
you still have the blobs and are willing to re-run every indexer over all of
history. Losing `blocks/` is losing the archive.

**Most of the KV rebuilds; some of it does not.** The blob catalog and the
reconciled part of the pin ledger are derived caches -- `bloard rebuild`
reconstructs the catalog from `blocks/`, and reconciliation rebuilds the ledger.
But the head roots (`h`), manifest tips (`m`), the writer IPNS sequence (`i`), a
follower's checkpoint and anti-replay floors (`f`), and the staging-pin leases are
current-selection, monotonic-publication, anti-replay, and time-bearing state that
**no walk of an unordered blockstore can reconstruct** (spec 6). The full
classification and its recovery consequences are §2.5; the KV prefix map is §2.2.

So: back up `blocks/` **and** the non-derivable KV, together and atomically (§4).
The KV is small, and the part of it that does not rebuild is exactly the part that
prevents a signed rollback or names which root is current.

### 2.2 KV prefix map

One Pebble store, shared by four packages, separated by single-byte prefixes.
Every key of one structure is kept clear of every key of another -- no key of one
can be a prefix of a key of another, so a prefix scan of the ledger cannot walk
into the catalog.

| Prefix | Owner | Key | Value | Rebuilt by |
|---|---|---|---|---|
| `c` | `catalog` (spec 6.1) | `'c' \|\| vh[32]` | blob `cid.Bytes()` | `bloard rebuild` |
| `p` | `catalog` (spec 6.2) | `'p' \|\| head \|\| 0x00 \|\| purpose \|\| 0x00 \|\| cid.Bytes()` | `flags[1] [\|\| expiry[8]]` | pin reconciliation -- EXCEPT `_staging` leases |
| `h` | `server` | `'h' \|\| head` | root `cid.Bytes()` | nothing -- see below |
| `m` | `server` (spec 10.5) | `'m' \|\| head` | manifest tip `cid.Bytes()` | nothing -- see below |
| `i` | `p2p` (spec 8.1) | `'i' \|\| "seq"` | IPNS sequence number | nothing -- see below |
| `f` | `follow` (spec 11.3) | global `'f'\|\|"updated_at"` and `'f'\|\|"ipns_seq"`; per-head `'f'\|\|"checkpoint:"\|\|head` (+ legacy `'f'\|\|"synced_to:"\|\|head`, `'f'\|\|"manifest:"\|\|head`) | uint64 floors; a versioned `{root, synced_to, updated_at, [manifest tip]}` checkpoint; legacy `manifest:` holds a manifest tip `cid.Bytes()` | nothing -- see below |

**Four dedicated prefixes are not rebuildable from the DAG** -- `h`, `m`, `i`, and
all of `f` -- and so is one set of rows under a fifth prefix: the `_staging` lease
rows inside the otherwise-derived `p` ledger (below). This is the short version;
§2.5 is the full state contract and its recovery consequences.

- **`h` (head roots)** is which root each head is currently serving. On a writer
  it is authoritative: lose it and the head resumes from *empty* and re-derives
  everything the indexers replay -- correct, but a full re-index, a very long day
  rather than data loss. On a follower the same `h` entry is a write-through mirror
  of the follower checkpoint (`f`), never a resume source.
- **`m` (manifest tips)** is which Manifest is each head's current chain tip
  (spec 10.5). Like `h`, nothing in the DAG says which of the Manifest blocks is
  current; a writer that loses it un-publishes its heads' filter attestations, and
  on a follower it too mirrors `f`. The chain blocks are pinned only via the recursive
  pin the reconciler derives from the CURRENT `m` (pinning/reconcile.go); lose `m` and
  that pin is dropped, so the old chain survives only until the next GC -- recover
  before GC runs, or the blocks are gone too. Recovery turns on whether you have the
  EXACT known schedule chain and every link still passes the append-only preflight at
  the head's current position. Manifests are content-addressed, so if you do, replay
  it in place with **both the external HTTPS read route withdrawn and IPNS off** (§4.6,
  §7.5 -- each intermediate document leaks over unauthenticated `GET /bloar/v1/heads`,
  not just IPNS): deterministic re-minting reaches the identical tip followers accept
  -- an empty tip bootstraps genesis first, a stale one replays forward from where it
  points. When any link is unknown, or a change now sits behind that boundary, a backup
  is the general path (§4.6).
- **`i` (IPNS sequence)** is the monotonic publication counter of spec 8.1 -- not
  a selector, not an identity, and it does NOT self-heal to any known point. The
  process persists the sequence but not its in-memory "last published CID", so while
  that marker is unset (a fresh process, before a success for the current document) an
  attempt that gets as far as PERSISTING the next number consumes it -- even if the DHT
  put then fails, the persisted increment stands; only a failure BEFORE that
  persistence (the store itself failing) consumes nothing (p2p/ipns.go). So the first
  SUCCESSFUL publish lands one OR MORE above the startup value, and only after it do
  later unchanged republishes reuse the number. There is thus no deterministic
  operator-controlled catch-up: a resolver holding a record ahead of a restored-behind
  `i` keeps rejecting the writer's lower record until the sequence happens to climb
  past it. §4.6 covers the recovery; the interim is the HTTPS channel.
- **`f` (follower checkpoint + floors)** is a follower's authoritative anti-replay
  memory (spec 11.3), and it is not one value. Per followed head there is a versioned
  **checkpoint** (`{root, synced_to, updated_at, and the manifest tip if any}`) that
  is the ONLY thing a restart resumes from. Beside it are a global document freshness
  floor (`updated_at`), bounded per-IPNS-name sequence floors (`ipns_floors:v1`),
  and the last admitted DNSLink name/signer delegation (`delegation:v1`). The next
  poll does **not** rebuild any of it. A
  poll can restore *a* signed state, but the floors are exactly what proves the
  polled state is at least as fresh as the last one accepted -- without them a
  correctly signed rollback is indistinguishable from progress. Not
  derivable, not "the next poll"; state to back up or, if lost, recover deliberately
  (§4.6). Independent names do not compare sequence numbers; the global document and
  per-head floors preserve content monotonicity across a name rotation.
  The same `f` prefix also contains `verified_segment:v1:<CID>` cache entries for
  sealed Segments successfully checked under `verify: full`. Those markers are
  disposable derived state, not anti-replay authority: losing one causes safe
  KZG re-verification. Open-Segment proofs are never persisted.
- **`_staging` leases (rows under `p`)** are the pin ledger's staging rows (the
  reserved `_staging` head, §2.3, spec 9): TTL-bearing leases reconciliation never
  touches and a rebuild cannot reconstruct. They are the one part of the
  otherwise-derived `p` ledger that does not come back.

A KV backup is not a nice-to-have "even though it rebuilds" -- it is how this
non-derivable state survives storage loss. It is small.

### 2.3 The pin ledger's reserved head

`p` keys are `'p' || head || 0x00 || ...`, and one `head` in there is not a
head: `_staging`, which is where ingest records blobs it has accepted but nobody
references yet (spec 9's window (a); see §5.2), and where a follower's fetch pass
records index nodes and blobs it has fetched but whose head pins have not landed
yet (spec 11.3; §5.1). Both drop their rows once the head's own pins retain the
blocks, and both lapse on `ingest.staging_ttl` if that never happens.

It cannot collide with a real head. Spec 3.1's head names match
`[a-z0-9][a-z0-9-]*`, which has no underscore in it and cannot start with one,
so no configured head, no head in any publication document, and no head any
writer could build can ever own those rows. Pin reconciliation never touches
them; GC is what expires and marks them.

### 2.4 flatfs

The blockstore's shard function is `/repo/flatfs/shard/v1/next-to-last/3`, fixed
at store creation (spec 6). It is recorded in the store and checked on open: a
store created with a different one is refused rather than opened, because reading
it with the wrong function would find no blocks and look like an empty archive.

Practical consequences:

- **inodes.** One file per block. A full-retention mainnet archive is tens of
  millions of files. Size the filesystem's inode count, not just its bytes --
  `df -i` is the one that will run out first on ext4 with defaults.
- **`ls` is not a tool here.** Do not run `du`, `ls -R`, or a recursive `chown`
  over `blocks/` casually; each is a walk of every file. This is why the systemd
  unit uses a dedicated user rather than `DynamicUser` (§3.2), and why there is
  no store-size gauge in the metrics (§6).

### 2.5 The KV state contract

"The KV rebuilds" is true of most of it and dangerously false about the rest, so
this is the exact contract. The KV holds five kinds of state. Only the first
recovers by **local re-derivation from the DAG** -- no external source, no network;
the others are not locally derivable, and their recovery, where it exists at all,
needs a backup or a conditional external re-index (§4.6, spec 6):

- **Derived caches -- locally rebuildable.** The blob catalog (`c`), the
  reconciled part of the pin ledger (`p`), and a follower's versioned
  sealed-Segment full-verification markers (`f` keys). `bloard rebuild`
  reconstructs the catalog by walking `blocks/` (§7.3); reconciliation rebuilds
  the ledger; ordinary follower sync recreates an absent verification marker
  after checking every `RefEntry`. Losing these costs work, not authority.
- **Current-selection state -- NOT locally rebuildable.** Head roots (`h`) and
  manifest tips (`m`): which Head/Manifest block each head is currently serving. On a
  **writer** these are authoritative restart selectors with **no import path** --
  nothing imports a root from a publication document. Recovering them is therefore
  NOT a local re-derivation: a lost `h` comes back only by re-indexing forward from
  complete *external* chain and blob sources, under GC protection and out of service
  until it catches up (§4.6); and `m` is re-derivable in place only if you have the
  EXACT known schedule chain and every link still passes the append-only preflight at
  the head's current position -- then replay it (§7.5, out of service), deterministic
  re-minting reaching the identical tip. Otherwise (any link unknown, or a change now
  behind that boundary) a backup is the general path (§4.6).
  On a **follower** both are exact **write-through mirrors** of the `f` checkpoint,
  re-derived from `f` on each adoption for the serve path and pin reconciler -- never
  a resume source.
- **Monotonic publication state -- NOT rebuildable.** The writer IPNS sequence
  (`i`, spec 8.1): a counter, not a selector or an identity. It is non-derivable and
  does not self-heal to a known point -- while the in-memory "last" marker is unset an
  attempt that PERSISTS the next number consumes it even if the DHT put then fails
  (only a failure before that persistence consumes nothing), so a rollback of `i`, or a
  rebuild to zero, has no deterministic catch-up past a higher external record (§4.6).
- **Anti-replay floors -- NOT rebuildable, and NOT re-established by the next
  poll.** A follower's checkpoint generation and freshness floors (`f`, spec
  11.3). Their entire purpose is to remember that the last accepted document was at
  least this fresh; a next poll can fetch *a* validly signed document but cannot,
  by definition, prove it is not an older one. The floors are the
  proof; nothing re-derives a proof of freshness after the fact.
- **Leases -- time-bearing, NOT rebuildable.** Staging pins (the reserved
  `_staging` head, §2.3, spec 9) carry a TTL. A rebuild cannot reconstruct a
  deadline; losing them lapses the staging pins early.

The rule that follows: a backup or restore must treat the four non-derivable
families as data, the same as `blocks/`. §4 documents how to capture them
consistently and how to recover when a family is lost or stale.

---

## 3. Keys and secrets

Three keys, three completely different jobs. None of them are interchangeable
and only one of them matters if it leaks in a way you cannot undo.

### 3.1 The auth token (`server.auth_token_file`, `archive.token_file`)

A bearer token, one line, guarding every endpoint that writes (spec 7.3). Read
endpoints are public. The one secret two processes share: bloard checks it on
every mutation, and every indexer presents it on every write.

```
# As root on the deploy host. Create the file 0600 root:root up front, so the
# secret is never even briefly readable -- no chmod/chown-after window under a lax
# umask -- then fill it (`>` to an existing file keeps its mode and owner):
install -m 0600 -o root -g root /dev/null /etc/bloar/token
openssl rand -hex 32 > /etc/bloar/token
```

The token exists as `0600 root:root` from its first inode, not `bloar:bloar`. Under the shipped systemd units nothing but
systemd itself reads this file, and it reads it as root; the daemons receive the
token another way. That way is the rest of this section, and it must remain
bounded and fail closed.

**The systemd path: a credential, not a file the daemon opens.** Both shipped
units carry

```
LoadCredential=token:/etc/bloar/token
```

which makes systemd read the source once at start (as root) and drop a private
copy, mode `0400` and owned by *that start's* service user, into a per-unit
directory it names in `$CREDENTIALS_DIRECTORY`. Nothing else on the host can read
that copy. The config then points at the copy, not the source -- and the key is
nested and different in each binary (there is no top-level `token_file`):

```yaml
# bloard: /etc/bloar/bloard.yaml
server:
  auth_token_file: "${CREDENTIALS_DIRECTORY}/token"
```

```yaml
# bloar-index: /etc/bloar/index/<instance>.yaml
archive:
  token_file: "${CREDENTIALS_DIRECTORY}/token"
```

and the daemon expands `${CREDENTIALS_DIRECTORY}` from its environment when it
reads the token.

The indexer is why this exists. `bloar-index@.service` runs `DynamicUser=yes`
(§2.4 explains why an indexer, unlike bloard, wants a throwaway uid), which
allocates a fresh, unnamed uid on every start. A `0600 /etc/bloar/token` owned by
any real account is unreadable to that uid, so the token read fails `EACCES`, and
`Restart=always` turns that into a deterministic crash loop on any host that
followed this runbook. Making the token world-readable would "fix" it by handing
every local process a credential that authorizes every archive mutation, which is
not a fix. The credential is: the token exists on disk only as `root:root`, and
reaches the one process that needs it as a copy no one else can open. bloard gets
the same treatment less out of necessity -- it runs as a named `bloar` user that
*could* read a `0600 bloar:bloar` source -- than for one story instead of two,
and so the token is never sitting at rest under an account a daemon compromise
would already hold.

`${CREDENTIALS_DIRECTORY}` is the **only** thing expanded, and only as an exact
leading prefix: this is not general variable interpolation, and a value that
merely contains a `$` elsewhere is an ordinary path. Expansion happens when the
token is **read**, not when the config is parsed. A service reads its token before
it serves anything, so a unit missing its `LoadCredential=` line still fails
closed at startup, naming the misconfiguration -- it never expands the unset
variable to nothing and reads a literal `/token`. But a command that consumes no
token loads the same installed config untouched: `bloard rebuild` (§7.3) is the
case that matters, and it now runs against the credential-form config with no
credential directory present. The one authenticated command an operator runs *by
hand* -- `bloar-index publish-manifest` (§7.5) -- has no credential directory
either; it takes a `-token-file` plain path or a `systemd-run` wrapper, both
documented there.

**The manual / container / docker-compose path: an ordinary file.** Off systemd
there is no credential directory, so use a plain path to a file the running uid
can read (the key is nested and differs between the two binaries):

```yaml
# bloard
server:
  auth_token_file: /etc/bloar/token   # readable by the uid bloard runs as
```

```yaml
# bloar-index
archive:
  token_file: /etc/bloar/token        # readable by the uid the indexer runs as
  max_put_blobs: 64                    # must equal bloard server.max_put_blobs
```

The container image (§8) runs as an unprivileged uid and reads a bind-mounted
token this way; the live docker-compose deployment is unchanged by any of the
above and stays exactly as it is. The shipped `deploy/examples/*.yaml` are the
systemd form (`${CREDENTIALS_DIRECTORY}/token`) with the plain-path alternative
called out in a comment on the same key -- the two are deliberately kept visibly
apart, because pointing the systemd form at a plain source or the plain form at a
credential directory each fails in its own way.

`archive.max_put_blobs` is deliberately duplicated from bloard's
`server.max_put_blobs`: it is the indexer's durable local safety bound,
not a second tuning knob. `index.max_put_blobs` must be at or below it. When the
archive is reachable the indexer requires its advertised value to match exactly;
when transport or HTTP 5xx exhausts the bounded request retries, the process
stays alive and exports `bloar_index_archive_available{head}=0`. A finalized
indexer reconstructs its stateless loop after one poll interval. The unfinalized
tracker instead keeps its in-memory observation of the last complete generation,
while the archive's selected generation remains the durable serving authority;
it retries the whole pass and never exposes a partial replacement.
Authentication, malformed requests, manifest/generation conflicts, and every
other authoritative 4xx remain fatal. This split keeps a cold publication writer
from crash-looping any indexer without turning a configuration or protocol
failure into an availability retry.
The availability metric and its alert are load-bearing, not informational: a
persistently malformed 200 is retried for the process lifetime, so losing that
signal would turn an observable degraded state into a silent one.

Required even on a follower, which writes nothing: the endpoints it guards
answer 403 for a followed head (spec 11.1 -- there is one writer per head and it
is somebody else), and an empty token is not "no auth", it would make every
request with an empty bearer token an admin. The daemon refuses to start with
one, credential copy or plain file alike.

**A limit worth stating plainly.** When it reads the token, a daemon sees only
the resolved bytes -- the credential copy, or the plain file it was pointed at.
It cannot see, and does not check, the permissions on the *source* `/etc/bloar/token`:
under the credential path it never touches the source at all. So there is no
in-process warning that would catch a source left `0644` by mistake. That check
belongs to deployment, and lives in
[`deploy/verify-token-credentials.sh`](../deploy/verify-token-credentials.sh).
Run as root on a deploy host, it derives its checks from the `User=`,
`LoadCredential=`, and `DynamicUser=` lines of the shipped unit files -- so a unit
with any of those removed or miswired fails it -- and then: starts a real `bloard`
as that `User=` with the token delivered exactly as the unit delivers it; runs a
real `bloar-index` under `DynamicUser` that, with its systemd-delivered credential,
performs an authenticated *mutation* (`publish-manifest` against a scratch head) --
accepted with the correct credential, `401` with a wrong one, refused before the
mutation with none; keeps that successful mutation unit holding under its
`DynamicUser` and, while it and `bloard` are both alive, asserts their identities
are non-root and distinct; and asserts the source is `0600` uid 0/gid 0 with no
ACLs and unreadable by both the `bloar` user and an unrelated one. It refuses to run without root and a systemd manager rather than
reporting a hollow pass. Run it after any change to these units or to the token's
on-disk permissions.

Rotating it: write the new token to the source (`root:root`, `0600`), then
restart the units -- `systemctl restart bloard` and `systemctl restart
'bloar-index@*'`. systemd re-reads the source on each start and re-delivers the
credential; there is nothing else to touch, and no config changes because the
config names the credential, not the secret. There is no overlap window, so do
the indexers promptly; they will retry, and their work is idempotent. (Off
systemd, replace the file the plain `token_file` points at and restart the
process the same way.)

### 3.2 The publication signing key (`publish.signing_key_file`)

Hex ed25519, either a 32-byte seed or a 64-byte private key, on one line. It
signs the publication document (spec 8). It identifies and authorizes one
publication writer. In legacy versions 1 and 2 it is also the only stable
archive-level identity a direct follower has. Version 3 separates those jobs:
`publish.archive_id` identifies the logical archive, while the follower's trust
configuration still decides which signing keys may speak for it.

```
openssl rand -hex 32 > /etc/bloar/ed25519.key
chmod 0600 /etc/bloar/ed25519.key && chown bloar:bloar /etc/bloar/ed25519.key
```

The public half is what direct URL/IPNS followers put in `follow.pubkey`; the
daemon logs it at startup. A DNSLink follower may instead learn it through the
authenticated DNSLink -> IPNS -> exact document CID chain. Setting
`follow.pubkey` alongside DNSLink pins this key and refuses automatic signer
rotation.

If this leaks, whoever has it can sign a publication document your followers will
believe wherever that key remains authorized. In version 3, matching the
`archive_id` does not make a different key trusted, but the ID cannot constrain
an already trusted key. Direct-key followers need an out-of-band config update.
An unpinned DNSLink trust root can rotate the IPNS name and document signer
together, but its default resolver does not validate DNSSEC; pinning the signer
is stronger and therefore deliberately makes rotation an operator action.

Back it up. A writer that loses it cannot continue that signing authority's
revision stream. The recovery is a new key and a conversation with every
follower. Version 3 lets the replacement retain the same logical `archive_id`,
but it does not silently authorize the replacement key.

#### Logical archive identity (`publish.archive_id`)

For a version-3 logical archive, generate the ID once:

```
openssl rand -hex 32
```

Put that exact 64-character lowercase value in `publish.archive_id` on every
independent writer of the archive. Do **not** generate one per writer. The value
is public, not a secret or credential; keep it stable and back it up with the
operator configuration. Changing it declares a different logical archive and is
not a signing-key rotation.

Setting `publish.archive_id` activates signed, revisioned publication version 3
even for a finalized-only writer. It therefore requires
`publish.signing_key_file`. Upgrade followers to a version-3-capable build before
enabling it: older readers reject an unknown major version rather than ignoring
the new signed field. Independent writers share only the archive ID and logical
head parameters. Each keeps its own signing key, revision state, store, libp2p
identity, URL, and IPNS name.

For the complete failure-domain topology, bootstrap proof, exact manifest
continuity, and add/remove/rotation/compromise procedures, see
[Independent writers](multi-writer.md).

The follower's authorized source/key list is the writer membership policy. The
publication document does not carry or update that roster. A safe planned key
rotation is:

1. Add the replacement source and public key to every follower while the old
   writer remains authorized and available.
2. Start the replacement writer with the same `archive_id` and compatible
   finalized heads. Wait until its claim is equivalent to or provably dominates
   the old writer's claim under spec 11.3.1.
3. Verify followers can admit and fetch from the replacement, including its
   recorded signer/source provenance.
4. Remove the old key and source from follower authorization, then retire it.

For a compromise, stop and de-authorize the key first. Revocation is not carried
in the archive and is not retroactive: removing a key prevents future admission,
but a generation already accepted from that writer remains accepted until an
operator audits the last trusted frontier and performs an explicit recovery.
Multi-writer comparison provides outage tolerance, not a quorum; one authorized
key can still publish a structurally valid false append.

### 3.3 The libp2p identity (`p2p.identity_key_file`)

Hex ed25519, same format. Defaults to `<store.path>/p2p.key`, created `0600` on
first run with the PeerID logged.

It must be stable across restarts. It is:

- the PeerID in every multiaddr the publication document advertises, and in every
  follower's `p2p.peers`;
- **the IPNS name.** Spec 8.1's records are signed by this key, so the IPNS name
  followers resolve is derived from it.

Lose it and the node comes back as a stranger: a new PeerID, a new IPNS name, and
every follower configured against the old ones fetching from nobody. It is in the
store directory, so a store backup carries it -- which is the argument for
leaving it at the default rather than moving it somewhere the backup does not
reach.

**Read-only key directories.** The shipped `bloard.service` runs
`ProtectSystem=strict` with `/var/lib/bloar` the only writable path, so a key
under `/etc/bloar` -- the signing key example above, or the identity key if you
point it there -- lives on a read-only filesystem to the daemon. Reading an
existing key there works: the daemon takes its lock on the key file's own inode
and writes nothing beside it. But **first-run creation needs a writable
directory** -- it installs the new key through a temporary file and a lock in the
same directory -- so a missing key under a read-only path fails with a clear
error rather than a silent stranger. Provision such keys ahead of time (the
`openssl` line above), or leave the identity key at its writable default.

> **One key, two jobs.** Spec 8.1 allows the libp2p identity and the document
> signing key to be the same key, and v1 does not stop you. Think before you do
> it: they have different blast radii and different rotation stories. The signing
> key is a publication authority to followers and rotating it is a conversation;
> the p2p key is a network address and rotating it is a reconfiguration. Fusing
> them means any reason to rotate either forces both. Separate files, separate
> keys, is the recommendation.

### 3.4 Embedded swarm discovery and resource budgets

An enabled `p2p` block joins the public Amino DHT and rendezvous discovery by
default. Bloar derives one stable deterministic namespace-block CID for each configured
`(network, head)` pair. It does **not** advertise every archive block and it
does not give the DHT to Bitswap as generic content routing. A provider result
is only an untrusted address lead; publication-document signatures, CIDs,
replay floors and configured/delegated signer state remain the trust boundary.

Current-pointer discovery is similarly narrow. A serving node coalesces and
provides only each configured head's current root, optional manifest tip, and
the exact authenticated source-document CID. A signed follower which republishes
its registry under its own IPNS name additionally retains and provides that one
node-local document CID; it does **not** replace the upstream source document.
This is what lets both a follower of the original writer and a follower of the
relay find the exact document they resolved after the writer is offline. The
hard bound is 64 real heads plus one node-local publication document, with one
process-wide DHT `Provide` start per second. Unsigned or refused documents never
enter the verified serving set.

On a local miss Bloar may query the DHT only for one already-authenticated
document/root/tip CID, connect the bounded provider leads, and retry that same
content-addressed fetch. It never turns the DHT into generic Bitswap routing and
never queries for descendants. Remote provider records cannot be withdrawn, so
an old record is only a stale lead: byte identity and the publication verifier
still decide what may be adopted.

Use `dht: {bootstrap: private}` to seed the DHT only from `p2p.peers`, including
for hermetic tests and intentionally private deployments. Use
`rendezvous: {enabled: false}` to keep only explicit peering/IPNS routing.
Setting `bitswap.serve: false` also suppresses rendezvous advertising, so a node
never claims to serve while its Bitswap server is disabled.

#### Split public edge transaction budgets

In the split topology, private `bloard` signs the document and IPNS record and
hands both to `bloar-edge` over its private AF_UNIX socket. The handoff has three
strictly ordered defaults:

| budget | config | default |
|---|---|---:|
| edge provider-before-IPNS transaction | `control.transaction_timeout` on `bloar-edge`, and the matching `publish.edge.transaction_timeout` on the writer | `2m` |
| writer's complete local request | `publish.edge.request_timeout` | `2m30s` |
| edge control-server write ceiling | fixed outer safety ceiling | `3m` |

The two-minute transaction keeps the former edge server limit as the bound on
actual DHT work. The writer then has 30 seconds for an AF_UNIX dial, the bounded
request body, and the structured response; the server retains another 30
seconds so its socket deadline cannot race the writer's. Equal adjacent values
are rejected. The writer transmits the expected transaction timeout and the
edge refuses a mismatch, so independently edited configs fail before any DHT
mutation.

Crossing the control socket is a bounded commit point. A caller cancellation
that arrives after the handoff does not abandon the HTTP request: the writer
waits for the edge's authoritative success or stage error, up to its request
budget. The edge transaction itself remains synchronous and context-bounded.
A `provide_document` timeout leaves the prior IPNS value authoritative. A
`put_record` timeout can occur after the new sequence/revision floor was
durably staged; restart or retry re-provides the exact document and repeats the
same signed record idempotently. The floor is never rolled back merely because
the DHT call failed.

Use `edge_publication_stage_duration_seconds{stage,outcome}` to tune this from
measurements. `stage` is `provide_document` or `put_record`; `outcome` is
`ok`, `error`, `timeout`, or `canceled`. This is edge-side DHT latency.
`ipns_publication_stage_total` remains the private writer's end-to-end result.
The two should be read together: a structured edge timeout is an actual bounded
stage result, while exhaustion of the longer writer request budget means the
edge did not return within the protocol's promised margin.

Circuit Relay v2 and DCUtR are also default-on inside an enabled `p2p` block,
but they have deliberately different activation rules. The capped relay service
is installed only while libp2p observes the node as **Public**; Bloar never
forces that reachability verdict. Hole punching is installed on every embedded
host, including dial-only followers. AutoRelay is not a public relay-discovery
mechanism: it remains off, visibly reporting zero candidates, until
`p2p.relay.static_candidates` supplies one or more full direct peer multiaddrs:

```yaml
p2p:
  relay:
    service: {enabled: true}
    hole_punching: true
    static_candidates:
      - /dns4/relay.example.org/tcp/4001/p2p/12D3KooW...
```

Multiple direct TCP/QUIC addresses for the same PeerID are grouped into one
candidate. Public deployments should use globally reachable IP or DNS
addresses; loopback/private addresses remain valid for private deployments and
hermetic tests but cannot become useful public advertised circuit addresses.
Remove the candidates to disable AutoRelay. Set `service.enabled: false` to opt
out of serving hop reservations, and `hole_punching: false` to opt out of DCUtR
(the latter is rejected while candidates remain configured).

The pinned service posture is 32 reservations, one reservation and four open
circuits per peer, 8 reservations/IP, 16/ASN, a 1-hour reservation TTL, 2-KiB
buffers, and a 2-minute/128-KiB-per-direction circuit limit. This is an
**introduction/control-plane path only**. A raw EIP-4844 blob alone consumes the
entire data allowance before Bitswap framing, and Bloar does not opt Bitswap
into libp2p limited connections. Successful archival transfer therefore means
DCUtR produced a normal direct connection (or another ordinary direct path
already existed); it never means a blob silently fell back through the relay.

The shipped resource policy is pinned rather than inherited from host-sized
libp2p defaults. `connection_manager.low_watermark=160`,
`high_watermark=192`, and `grace_period=1m` are pruning policy: after a burst,
ordinary connections are trimmed toward the low watermark. They are **not**
hard caps, and configured static peers are protected from pruning in addition
to the low target. Config validation refuses a high watermark that cannot fit
the low target plus unique protected peers and a pruning slot.

The independent resource manager is the hard boundary: 512 MiB, 1024 file
descriptors, 256 total connections (256 inbound/256 outbound), and 4096 streams
(2048 inbound/3072 outbound). One peer is capped at 128 MiB, 16 descriptors,
8 connections, and 512 streams (256 inbound/512 outbound). Directional and
total ceilings apply simultaneously. Zero selects these defaults; negative or
internally inconsistent values fail before host construction. The complete
spelling is in the writer and follower example configs. These values are
conservative provisional defaults, not field-derived capacity claims; change
them only after production rollout measurements.

Core observability is deliberately bounded: `bloar_p2p_live_peers` uses only
closed `direction` and `transport` labels, and
`bloar_bitswap_scheduled_bytes_total` uses only `static`, `rendezvous`, `relay`
and `other`. No PeerID is a label. The latter counts block payload put into a
Boxo outbound envelope before the network write, so it is attempted/scheduled
payload—not delivery acknowledgement or proof of serve-back.

Supported libp2p collectors register only with Bloar's private Prometheus
registry (or are disabled when metrics are off). With pinned go-libp2p v0.48,
AutoNATv2's auxiliary blank host still registers exactly five
`libp2p_eventbus_*` collectors in the process default registry because upstream
exposes no registerer/disable seam there. Bloar never swaps
`prometheus.DefaultRegisterer`; this narrow exception is regression-tested and
must be removed upstream or in a pinned fork before claiming complete registry
isolation.

---

## 4. Backup and recovery

### 4.1 The unit of backup is the whole store

Back up the **whole store as one point-in-time unit**: `blocks/`, the KV, and the
default `p2p.key`, together. Not `blocks/` alone -- the KV carries non-derivable
current-selection, publication, and anti-replay state that no rebuild reconstructs
(§2.5). Two capture methods are supported, and both take the store whole:

- a **stopped-daemon copy** (§4.2), the baseline that always applies; and
- an **atomic filesystem snapshot** of one dataset (§4.3), which supports online
  capture but only under a strict condition.

Some recovery inputs are **not** in the store and are backed up separately (§4.6):
the config, the auth token, any externally stored publication signing key, and a
custom external `p2p.identity_key_file`. A store snapshot carries the default
in-store `p2p.key` and nothing under `/etc`.

### 4.2 Stopped-daemon copy (the baseline)

The one method that always works: stop the daemon, copy the whole store
directory, restart.

```
systemctl stop bloard
cp -a /var/lib/bloar /backup/bloar-$(date -u +%Y%m%dT%H%M%SZ)   # KV + blocks/ + p2p.key
systemctl start bloard
```

**Copying a running store is NOT a backup.** Pebble is writing the KV as you read
it, so a file-by-file copy of a live store can capture a torn or internally
inconsistent KV -- a half-written manifest set, a root without its follower floor.
Stop the daemon, or use an atomic snapshot (§4.3); never a plain `cp` of a live
store.

### 4.3 Atomic filesystem snapshot

An atomic filesystem snapshot (ZFS, LVM) captures the store online **iff one
atomic snapshot covers the entire store** -- KV, `blocks/`, and `p2p.key` -- on a
**single dataset/volume**. That single snapshot is crash-consistent, and crash
consistency is exactly what bloard is built to survive (§4.5), so a restored
snapshot is a bloard that was power-cycled, which is a case with tests.

**If the KV and `blocks/` live on separate filesystems, this path is
UNSUPPORTED.** Two snapshots taken of two filesystems are not atomic *across* them:
one can capture a root that the other's follower floor or catalog does not yet
reflect, which is the torn state §4.2 warns about. On a split layout the only
backup is the stopped-daemon copy. Keep the whole store on one dataset (§4.4) so
the atomic path is available.

There is **no supported online copy of a live single store short of a snapshot.**
In particular there is no "hold the GC gate, checkpoint Pebble, copy the files"
procedure: the GC gate is an in-memory library lock (not an external control), the
daemon holds the Pebble lock so no second process can open the KV, and there is no
operator-invocable checkpoint. An in-daemon authenticated backup operation is
possible future work; until it exists, the two methods above are the whole story.

### 4.4 ZFS: the recommended one-dataset layout

The recommendation is a ZFS dataset per store, holding KV and `blocks/` and the
key together -- which is both what makes §4.3's atomic snapshot correct and what
lets ZFS tell you when the disk disagrees with bloar's content-addressed failure
model.

```
zfs create -o recordsize=128K -o compression=off -o atime=off tank/bloar
```

**`recordsize=128K`.** This is the interesting one, and it is not the default's
128K by coincidence -- it is an exact alignment. An EIP-4844 blob is exactly
`BLOB_SIZE` = 131072 bytes = 128 KiB (spec 1), and flatfs stores one blob per
file with no framing. So a blob block is exactly one ZFS record: one read, one
checksum, no read-modify-write on the way in, and no partial-record tail. The
index blocks (dag-cbor, ~1 KiB to ~1.5 MiB) do not align this neatly and do not
need to -- they are a rounding error next to the blobs, and ZFS stores a
short file in a short record anyway.

Do not lower it. At 16K, every blob write becomes eight records and every blob
read eight checksummed lookups, for no benefit at all: nothing ever partially
rewrites a blob, because a blob is content-addressed and rewriting it would make
it a different blob.

**`compression=off`.** Blobs are dense: they are either random-looking rollup
data or KZG-verified field elements. LZ4 will spend CPU on every write to find
nothing. If a dataset holds *only* index blocks it would compress well, but
that is not a dataset you have.

**`atime=off`.** Tens of millions of files, every read a metadata write. No.

### 4.5 Snapshots and incremental send

Blocks are immutable and content-addressed: a block never changes, it is only
created or (by GC) deleted. That makes an incremental `zfs send` close to
optimal -- the delta between two snapshots is genuinely the new blobs plus the
new index spine, with no rewritten data in it at all.

```
zfs snapshot tank/bloar@$(date -u +%Y%m%dT%H%M%SZ)
zfs send -I tank/bloar@<previous> tank/bloar@<latest> | ssh backup zfs recv -F tank/bloar
```

**Crash consistency is what a snapshot preserves.** An atomic snapshot of the one
dataset (§4.3) is exactly what bloard is built to survive: every block write is
durable before the root that names it swaps (spec 5), the pin ledger's write order
means a crash leaves extra pins rather than missing ones (spec 6.2), and Pebble
journals. A restored snapshot is a bloard that was power-cycled.

**Atomic consistency is NOT freshness.** A snapshot is a consistent picture of one
instant; restoring an *older* one rolls the node back to that instant and discards
everything after it. That matters because some of the KV is anti-replay state whose
whole job is to move forward (§2.5):

- On a **writer**, an old snapshot rolls `h`/`m`/`i` back below what downstream
  followers already accepted and below the IPNS sequence already published. It
  reopens the rollback exposure for the whole post-snapshot interval: the writer
  now serves and may re-sign roots older than ones the world has seen.
- On a **follower**, an old snapshot rolls its authoritative `f` checkpoint and
  freshness history backward, so it will again accept documents it had already
  advanced past.

A snapshot does **not** carry post-snapshot anti-replay knowledge forward -- there
is no way for the restored node to know what it accepted or published *after* the
snapshot instant. Recovery from a stale snapshot therefore has extra steps (§4.6);
prefer the newest consistent snapshot.

**Do not snapshot during a GC if you can help it.** Not for correctness -- a
half-swept store is a store with garbage in it, which is where it started -- but
because a sweep deleting blocks during a snapshot maximises the delta between
that snapshot and the next.

**Scrub on a schedule.** `zpool scrub` is how bit rot is found, and this is data
whose whole value is that it is exactly right. A follower that reads a corrupted
block notices immediately (the CID will not match), which is a good property but
a bad way to find out.

### 4.6 What a restore gets you, and the freshness gap it does not

Restore the whole store (KV + `blocks/` + `p2p.key`) from a **current** consistent
capture and you have the node you captured. The rest of this section is what to do
when the capture is *stale* or *partial* -- the freshness gap §4.5 warns about. Only
executable paths are documented; where a gap closes only with a tool that does not
yet exist, that is stated as a limitation, not papered over with a procedure.

**Writer selectors (`h`, `m`) lost or stale.** The **general in-place recovery is to
restore a newer consistent snapshot** that still holds the selector. There is **no
command that imports a selector** -- a writer never adopts a root or a tip from a
document, so nothing re-installs the "current" pointer directly -- and the re-index
and rebootstrap paths below are narrower, each with conditions:

- **`h` (head roots).** A re-index forward is possible but **conditional**. A writer
  with `h` gone resumes each head from *empty* and the indexers rebuild it, but only
  when (a) **complete historical chain and blob sources** are available to re-fetch
  from (see "Restoring `blocks/` alone" below -- it is a full re-fetch, not a local
  reshuffle), AND (b) the otherwise-unpinned old blocks are **protected from GC** for
  the whole run. And keep the node **out of service until the re-index has caught
  back up to the last root/`synced_to` it had published**: until then it serves and
  re-signs roots behind what downstream followers already accepted, reopening the
  rollback window (§4.5). A *stale* `h` from an old snapshot has the same hazard --
  it resumes from the stale root and re-indexes forward, but is behind its own
  published state for the entire catch-up window.
- **`m` (manifest tips).** Recovery turns on ONE question: do you have the EXACT
  known schedule chain, and does EVERY link still pass the append-only preflight at
  the head's current position? Manifests are content-addressed functions of
  `{head, sources, prev}`, so re-publishing a known schedule re-mints the identical
  CID.
  - **If yes**, reconstruct in place -- but for the ENTIRE replay both **WITHDRAW the
    external HTTPS read route** AND run with **`publish.ipns: false`**; "out of
    service" alone is not enough. Every manifest POST rebuilds the publication document,
    which `Heads.rebuild` both hands to the IPNS publisher (`OnDoc`) AND stores in the
    document that **unauthenticated** `GET /bloar/v1/heads` serves (server/heads.go,
    server/bloar.go); IPNS off closes only one channel, so a fresh or low-floor follower
    could still adopt an intermediate genesis/ancestor document over HTTPS. Block the
    external read route at your proxy (leave only the loopback address `publish-manifest`
    uses), and restore EITHER channel only after the exact FINAL tip CID is restored and
    verified. Then: from an **absent** tip, bootstrap the original genesis (this first
    POST skips the append-only check -- `preflightManifest` runs no `ValidateUpgrade`
    with no published predecessor, and a successor on an empty/uncovered head skips it
    too, since no ground is frozen; structural/source validation still applies) and
    replay each known successor with `publish-manifest` (§7.5); from a **stale
    ancestor** tip, first verify its block is actually READABLE (the selector is only a
    CID -- being the current `m` does not make an absent or corrupt block readable),
    then replay forward from where it points. Once the block is present and reconciled,
    the recursive current-`m` pin RETAINS it across GC. Each covered-head successor POST
    re-validates the schedule against the head's current L1 position (`ValidateUpgrade`)
    and re-mints the next link, in order, up to the identical tip followers hold --
    which they accept as an equal tip.
  - **If no** -- any link unknown, a historical change now **behind the append-only
    boundary** (frozen, so the preflight refuses it), or exact replay cannot reproduce
    the CID -- there is **no in-place recovery**: restore a newer consistent snapshot
    that still holds the tip. A scoped selector/chain import would be a code change, so
    do not script one.

  Re-minting only the genesis for a chain that has upgrades is NOT recovery: it
  publishes a tip that does not descend from the one existing followers hold, so they
  refuse it with `manifest_ancestry` (§6.1, §7.5).

If none of the paths above is available -- a newer snapshot, a re-index (for `h`), or
a known-chain replay (for `m`) -- the only remaining move is a **disruptive last
resort**, not an in-place repair: abandon the head's published
identity and start a fresh one -- a new publication, and if the p2p identity also
changes, a **new IPNS name every consumer must be repointed to**, with the full
name-rotation blast radius below (HTTPS interim, direct IPNS consumers reconfigured,
followers re-bootstrapped).

**Writer IPNS sequence (`i`) lost or stale.** In-place recovery is safe when the
restored `i` is **at or above** the last sequence the writer externally published:
persistence precedes the DHT put, so an `i` running ahead just publishes higher
still. When `i` is behind, do NOT rely on a self-heal to close the gap. While the
in-memory "last" marker is unset, an attempt that gets as far as PERSISTING the next
number consumes it -- even if the DHT put then fails, the persisted increment stands;
only a failure before that persistence consumes nothing (the increment is stored before
the put, and the marker is set only after it succeeds; p2p/ipns.go). So the first
successful publish lands one OR MORE above the startup value, and only later successful
unchanged republishes reuse it. The advance is therefore an unpredictable amount, not a
known one -- there is no deterministic operator-controlled catch-up, and a resolver
holding a higher record keeps rejecting the writer's lower one until the sequence
happens to climb past it. The executable
recoveries do not depend on that happening: (1) restore a **newer** consistent
snapshot whose `i` is at or above the last published sequence, or (2) **rotate the
IPNS identity** (a new `p2p` key is a new IPNS name) and repoint consumers, HTTPS as
the interim. This repairs
*publication* only. It does nothing for
a follower's freshness -- and, as the next item shows, a name rotation is itself only
half a fix on the follower side.

**Follower checkpoint/floors (`f`) lost or stale.** A follower that lost `f`, or
restored a stale one, has lost the anti-replay memory a next poll cannot rebuild
(§2.5). Two executable recoveries:

1. restore a **newer** consistent snapshot whose `f` is at least as fresh as the last
   document the follower accepted; or
2. **fresh-bootstrap the follower deliberately.** This is a fresh EMPTY store (or
   real tooling if it is ever built), suitable for a **pure follower only** -- a node
   that writes no heads. It is NOT a selective operation on a mixed-role KV: `f`
   shares the KV with everything else, so there is **no scoped follower-reset
   command**, and clearing the KV on a node that also writes heads would destroy the
   writer's authoritative `h`/`m`/`i`. A scoped reset that clears one head's `f`
   without touching writer state is a code change that does not exist today -- flag
   it rather than improvise. On a pure follower, concretely -- and note the store is
   typically its own dataset/mount (`RequiresMountsFor=/var/lib/bloar` in the shipped
   unit), which **cannot be renamed**, so the reset PIVOTS config paths and never
   moves the mount:
   1. **Retain the old store in place** -- do not rename or delete it. Create a fresh,
      uniquely-named store directory UNDER the mount and repoint `store.path` at it;
      both generations coexist, so rollback is a config change, not a restore.
   2. **Pivot the identity key too.** The shipped follower sets `p2p.identity_key_file`
      EXPLICITLY (to `/var/lib/bloar/p2p.key`), so changing only `store.path` would
      leave it reading the OLD key -- point it at `$FRESH/p2p.key` in the same edit.
      Copy the old key there for identity continuity, or leave that path absent to
      come back as a new peer (the daemon mints one on first run).
   3. Restart. The follower re-adopts from the publication document from scratch: all
      `f` state -- the per-head checkpoint, the freshness floor, and the global
      IPNS-sequence floor -- is **knowingly discarded and rebuilt from the first
      verified adoption**, and it re-fetches every block its pin policy retains (real
      bandwidth). The cost is explicit and accepted: the anti-replay interval since
      the last good checkpoint is gone.

   Concretely, with the shipped `bloard.service`, the `bloar:bloar` user, the config
   at `/etc/bloar/bloard.yaml`, an old store at `/var/lib/bloar`, and the private
   `server.metrics_listen` (here `127.0.0.1:9550`):

   ```bash
   set -euo pipefail   # FAIL CLOSED: any prerequisite step that errors stops the reset

   # PURE FOLLOWER ONLY -- do NOT run on a node that also writes heads (it would
   # discard the writer's h/m/i along with the follower floors).
   OLD=/var/lib/bloar
   METRICS=127.0.0.1:9550                 # your server.metrics_listen
   CONF=/etc/bloar/bloard.yaml

   systemctl stop bloard

   # 1. Fresh store UNDER the mount, created ATOMICALLY (mktemp fails rather than
   #    reuse an existing dir -- reusing one could preserve the very `f` state this
   #    reset must discard). The old store stays in place at $OLD for rollback.
   FRESH=$(sudo -u bloar mktemp -d "$OLD/rebootstrap.XXXXXX")

   # 2. Identity: the shipped config sets p2p.identity_key_file EXPLICITLY, so it is
   #    pivoted WITH store.path in step 3. For continuity, seed the fresh key from the
   #    old one now; SKIP this one line to come back as a new peer (the daemon mints it).
   install -o bloar -g bloar -m 0600 "$OLD/p2p.key" "$FRESH/p2p.key"

   # 3. Back up the config to an ATOMICALLY-RESERVED unique dir (mktemp fails rather
   #    than reuse or clobber -- a fixed name collides on a same-second retry), print
   #    it, then edit. Set BOTH store.path AND p2p.identity_key_file to $FRESH.
   BACKUPDIR=$(mktemp -d "$CONF.pre-rebootstrap.XXXXXX")
   cp -a "$CONF" "$BACKUPDIR/bloard.yaml"
   echo "config backed up to: $BACKUPDIR/bloard.yaml   (use this exact file to roll back)"
   echo "in $CONF set:  store.path: $FRESH"
   echo "          and  p2p.identity_key_file: $FRESH/p2p.key   (absent path => new peer)"
   "${EDITOR:-vi}" "$CONF"

   # Machine-check BOTH pivoted values with a YAML-aware parser BEFORE start -- an
   # editor exit code proves nothing, and the post-start $FRESH/p2p.key check cannot
   # prove the identity pivot in the continuity branch (step 2 pre-seeded that file).
   # (mikefarah yq shown; substitute your YAML tool. For a NEW peer, expect the same
   # $FRESH/p2p.key value -- the daemon creates the key at that pivoted path.)
   [ "$(yq '.store.path' "$CONF")" = "$FRESH" ] \
     || { echo "FAILED: store.path is not $FRESH in $CONF" >&2; exit 1; }
   [ "$(yq '.p2p.identity_key_file' "$CONF")" = "$FRESH/p2p.key" ] \
     || { echo "FAILED: p2p.identity_key_file is not $FRESH/p2p.key in $CONF" >&2; exit 1; }

   # 4. ARM a fail-closed cleanup BEFORE starting: any nonzero start job, `set -e`
   #    abort, or interrupt now stops bloard (which also clears a Restart=on-failure
   #    retry, bloard.service) while preserving the failure status. The handler is ONE
   #    idempotent path: it captures the status, disables its own EXIT trap, and IGNORES
   #    cleanup-time signals -- so a second INT/TERM/HUP during the (up to
   #    TimeoutStopSec) `systemctl stop` cannot kill the shell mid-cleanup and leave the
   #    half-pivoted service (or its restart retry) running. Disarmed only after ALL
   #    postconditions pass, so a clean run leaves bloard running.
   cleanup() { local rc=$?; trap - EXIT; trap '' HUP INT TERM QUIT; systemctl stop bloard || true; exit "$rc"; }
   trap cleanup EXIT
   systemctl start bloard

   # Poll the PRIVATE /readyz. It returns 200 only once EVERY configured followed head
   # has registered, so this is the AGGREGATE adoption proof -- no
   # per-head loop needed. Every curl is BOUNDED so the loop cannot hang; the readyz
   # loop is <=60 x (5s curl + 5s sleep) ~= 10 min worst case. On exhaustion, exit
   # nonzero and let the trap stop bloard (fail closed).
   ready=0
   for _ in $(seq 1 60); do
     if curl -sf --connect-timeout 3 --max-time 5 "http://$METRICS/readyz" >/dev/null; then ready=1; echo "ready"; break; fi
     sleep 5
   done
   [ "$ready" = 1 ] || { echo "FAILED: /readyz never 200 -- reset did NOT complete" >&2; exit 1; }

   # The editor exiting 0 does NOT prove the pivot took, and a stale store.path would
   # bring the OLD `f` state back with readyz still green. Assert the daemon actually
   # created the FRESH layout; if not, the edit did not land (the trap stops bloard).
   for want in kv blocks p2p.key; do
     [ -e "$FRESH/$want" ] || { echo "FAILED: $FRESH/$want absent -- config did not pivot to $FRESH" >&2; exit 1; }
   done

   # All postconditions passed -- disarm the fail-closed trap so a clean exit (and the
   # best-effort spot-check below) leaves bloard running.
   trap - EXIT

   # Optional, BEST-EFFORT spot-check that a head tracks the writer. The follower polls
   # every follow.poll_interval (60s in the shipped config) and the live writer keeps
   # advancing, so a short-window mismatch is EXPECTED, not a fault: read both sides
   # together (bounded curls) and retry the PAIR across a few poll intervals -- worst
   # case 5 x (15s + 15s curls + 30s sleep) = ~5 min. Only persistent non-convergence
   # is worth investigating -- /readyz above is the real pass criterion.
   WRITER=https://archive.example.org     # your follow.url
   HEAD_NAME=all                          # a head from your follow.heads
   for _ in $(seq 1 5); do
     mine=$(curl -sf --connect-timeout 5 --max-time 15 "http://127.0.0.1:8550/bloar/v1/heads/$HEAD_NAME" | jq -r .root || true)
     theirs=$(curl -sf --connect-timeout 5 --max-time 15 "$WRITER/bloar/v1/heads/$HEAD_NAME" | jq -r .root || true)
     [ -n "$mine" ] && [ "$mine" = "$theirs" ] && { echo "$HEAD_NAME tracking ($mine)"; break; }
     sleep 30
   done
   # (Follow progress in a SEPARATE terminal if you want: journalctl -u bloard -f)

   # Rollback -- a config pivot, NOT a restore; BOTH generations are kept:
   #   systemctl stop bloard
   #   cp -a "$BACKUPDIR/bloard.yaml" "$CONF"   # store.path + identity_key_file back to $OLD
   #   systemctl start bloard
   #
   # Reclamation depends on the OUTCOME, and is never part of this block:
   #  - After ROLLBACK, $FRESH is the abandoned generation:  rm -rf "$FRESH"  is safe
   #    (a self-contained child dir).
   #  - After a RETAINED reset, $FRESH is now LIVE and the OLD generation's files
   #    (blocks/, kv/, p2p.key) sit DIRECTLY under $OLD beside it. Reclaiming those is a
   #    separate, STOPPED migration (stop bloard, move data, repoint store.path, restart)
   #    -- do it deliberately, later.
   # NEVER `rm -rf "$OLD"`: $FRESH is nested under it, so that deletes the live store.
   ```

   This clears every global, per-head, per-name and delegated-signer floor. It is
   therefore still a deliberate loss of anti-replay history, not a routine way to
   rotate a name.

**IPNS-name rotation.** Current followers keep a bounded sequence floor per IPNS
name because independent keys have unrelated sequence spaces. DNSLink is the safe
automatic path: update its single TXT target to the replacement `/ipns/<name>`;
the follower authenticates that record and exact document, then atomically commits
the new name and signer with the admitted generation. A return to a recently used
name retains that name's old floor. The bounded MRU retains 32 names and protects
the current delegation from eviction; even an evicted name cannot roll content back
because global document and per-head floors remain. With `follow.pubkey` set, name
rotation is allowed only when the document signer remains pinned. Direct
`follow.ipns` remains an explicit config change and still requires the configured
signer; HTTPS is the availability fallback, not a source of signer rotation.

**Restoring `blocks/` alone (the KV was on a different, lost disk).** You have the
archive but not the daemon's memory of it. The derived caches come back; the
selectors do not, and the re-index is neither free nor automatically safe:

1. `bloard rebuild -config ... -clear` rebuilds the catalog (§7.3).
2. Head roots (`h`) are gone, so on a **writer** each head resumes empty and must be
   re-indexed. This is a **full re-fetch, not a local reshuffle**: the beacon indexer
   re-fetches every block-attested blob from its live upstream sources and re-POSTs
   the bytes (there is no catalog-only reindex shortcut), so it costs upstream
   bandwidth and needs those sources available.
3. **GC must be held off during the re-index.** With `h` and the pin ledger gone, the
   restored blocks are unpinned, and a scheduled GC (`store.gc_interval`, default 24h,
   §5.1) marks from the empty ledger and would sweep the very blocks the re-index
   still needs. Ensure the re-index completes (which repins as heads advance) before a
   GC runs -- widen `gc_interval` for the duration, or keep the window shorter than
   it.
4. The pin ledger reconciles itself on the first reconciliation (once heads have
   roots again).
5. Losing the authoritative checkpoint/floor `f` records means a **follower**
   must fresh-bootstrap as above; losing only `verified_segment:*` cache records
   merely causes full-verification work to repeat. A lost `i` means the IPNS
   recovery above. None of the authoritative state is restored by a rebuild.

Keep the KV on the same dataset as the blocks so a single snapshot has both (§4.4);
it is small and there is no reason to separate them.

**Keys and external inputs are a separate restore.** A whole-store snapshot is
**data-state** recovery. It carries the default in-store `p2p.key`, but not the
inputs kept outside the store, each of which is its own recovery item: the config,
the auth token/credential (§3.1), any externally stored publication signing key
(§3.2 -- losing it is a new key and a conversation with every follower, not a
restore), and a custom external `p2p.identity_key_file` if you moved it off the
default (§3.3). Back these up where they live, not by assuming the store snapshot
reached them.

---

## 5. GC, integrity scrub, and staging

### 5.1 Online GC and integrity scrub

An online epoch mark-and-sweep owned by bloard, on `store.gc_interval` (default
24h, spec 9). It takes one short mutation/publication cut, not one lock for the
full archive walk:

1. At the start barrier **T0** it finishes in-flight mutations and
   block-materializing HTTP reads which selected a root before the cut;
   then it reconciles pins, expires stale staging rows, snapshots the pin groups,
   and activates an in-memory protection epoch.
2. With writers running again, it builds M from the T0 pin snapshot and
   enumerates flatfs. Application block reads and writes after T0 add their
   multihash to the epoch's touched set T. A block may be deleted only when it
   is absent from M ∪ T;
   the final T check and deletion are serialized per key with application
   protection.
3. After enumeration, a blockstore-lifecycle barrier waits for in-flight block
   operations and closes the epoch. Deletion is already over, so it does not
   stop a whole root-publication transition.

The application and collector deliberately use different views of the same
blockstore. The application view protects keys touched during the run; the
collector view is untracked, otherwise the collector's own reads would put the
entire archive in T. `apply_refs`, `Truncate`, reconciliation, ingest, and the
follower's adoption or checkpoint `Resume` publication share the cut gate.
This is what makes a completed-before-cut root part of M and a published-after-
cut root's blocks part of T.

Application enumeration has one operational wrinkle. An `AllKeysChan` begun
while idle holds a lifecycle lease until you drain its returned channel or
cancel its context; a new T0 waits. An application enumeration attempted during
an epoch is refused rather than allowed to stream unprotected keys. GC and scrub
use a different, untracked iterator which preserves asynchronous flatfs errors.
If a collector is unexpectedly stuck before its cut, look for an embedded caller
which abandoned an application key channel without cancelling it.

The public blobs read and manifest GET also take a bounded reader lease on this
same cut. It starts only after blobs-response memory admission, immediately
before root/tip selection, and ends after all index/blob/manifest bytes needed
by the response have been read and encoded. The first `WriteHeader` or `Write`
releases it before touching the client; cancellation and no-write exits release
it by deferred cleanup. Therefore an old request cannot lose a descendant when
its mutable generation or manifest tip is concurrently replaced and unpinned,
while a slow response body cannot delay T0.
`bloar_beacon_read_duration_seconds` already includes any wait to acquire the
lease and is the request-side latency signal; `bloar_gc_duration_seconds`
includes the collector's wait for pre-cut readers. Those two existing
bounded-label histograms expose both sides of contention without a second timer
around the same interval. Metadata, publication, and generation-status endpoints read
only static, pre-rendered, or KV state and take no reader lease.

Follower publication uses a monotonic collection generation as its race token;
the value advances at the start of every protection epoch and remains advanced
after that epoch ends. When the store reports no active protection epoch
(`ActiveEpoch() == 0`), adoption and ordinary startup `Resume` do **not** walk
the retained closure just to publish it: they capture the generation, take the
cut gate at commit, and refuse before any durable write if the generation changed.
The commit then performs validating `Get`s of the local root and manifest tip
before exposure.
For a window-policy `Truncate`, those validating reads cover both the raw
references surviving in the rebuilt target Segment and every sealed Segment
closure which the rewind moves newly inside the trailing window. Full retention
already included those closures in M; none does not retain them. These are `Get`
checks, not `Has` checks, so corrupt local bytes stop publication as well as
missing ones.

An adoption or concurrent `Resume` whose protection step observes an active
collection epoch takes the deliberate slow path: it walks exactly the closure
selected by that head's retention policy plus its manifest chain through the
protected application view, staging anything it fetches. The follower transition
lock stays held so another Poll, Resume, or quarantine action cannot replace the
plan during the proof.
Existing reads keep serving the last committed generation and GC continues; only
other follower transitions may wait on the closure's local I/O or network
fetches. This extended transition-lock hold is expected only for the rare
active-GC overlap. If the generation moves, the candidate is retried rather than
partially published.

The same boundary rule applies to an ordinary follower retention sync. If its
walk crosses a collection generation, it leaves the root and manifest fetched
markers unchanged and stale, and does not drop the staging pins accumulated by
that attempt. A later poll retries in the new generation. This may temporarily
retain extra staging data, but it cannot produce a dangling published root.

Exposure also clears the root completion marker whenever the root changes, and
the manifest marker whenever the tip changes. That remains necessary even when
a CID recurs: A → B → GC → A must rewalk A because A-only descendants could have
been collected while B was current. Generation stamps invalidate a proof across
T0; marker reset invalidates it across adoption transitions.

Do not confuse this generation-scoped presence work with `verify: full`.
`verifiedSegments` is a separate semantic cache: the first successful ordinary
walk checks every `RefEntry` of a Segment, including local blob hits, and later
walks may reuse that immutable CID-bound proof after a GC generation change or a
refetch of the same CIDs. Proofs for sealed Segments are stored under versioned
CID keys in the follower's checksummed KV and normally survive restart. The
write is deliberately non-synchronous: losing a recent marker on power loss only
repeats verification. Open-Segment proofs stay memory-only to avoid one durable
key per transient open CID; a proof is promoted if that CID later becomes
sealed. Changing the verification rule changes the key version. The faster
protection-only walk used for an active-GC adoption or Resume neither performs
KZG verification nor marks a Segment semantically verified; the subsequent
ordinary sync still owes that work.

Production uses the epoch-aware store described above. An embedded compatibility
blockstore which cannot expose an active epoch and collection generation proves
the same property conservatively by holding the whole publication/GC gate while
it walks the retained closure and records completion for ordinary sync,
adoption, or Resume. It disables the shared presence memo and rewalks under the
gate because no monotonic token can invalidate a pre-GC proof. That fallback can
block writers for the walk and should not be mistaken for production behavior.

There is no blockstore or pin-ledger conversion to perform when enabling this
collector. M and T are process memory for one run. The optional follower
verification markers are additive derived Pebble keys whose absence is valid,
so they need no migration. If bloard stops during a run, no collector continues
deleting; the next process starts from a newly reconciled T0.

Normal ingest and root advancement should now pause only at the start cut and
at a contended delete of the same key; an individual block operation can also
briefly meet epoch close. They no longer wait for the mark's whole I/O duration.
Alert on sustained publication/ingest latency during GC: it indicates an
unexpectedly long cut or lock contention, not an expected multi-hour
maintenance window.

To request one live reachability pass without changing the recurring schedule,
send `SIGUSR1` to the running `bloard` process. In Docker, for example:

```sh
docker kill -s USR1 <bloard-container>
```

This uses the same online collector and context as the scheduler. It is
serialized with scheduled GC and scrub, and a burst of signals is coalesced.
Host/process permissions are the authorization boundary. Watch for
`operator-triggered gc requested`, then the ordinary phase and completion logs;
the signal acknowledges a request, not successful completion. Do not force a
pass by temporarily shrinking `gc_interval`: if a long run crosses another
ticker edge, the scheduler can retain pending work and start a redundant
pass immediately afterward.

Tuning:

- **Writers** usually still want it infrequent. Their only garbage is orphaned
  roots and abandoned ingests; daily is generally enough, and a full key walk
  consumes I/O even though it no longer holds the write path.
- **Followers with a `window` policy** want it more often -- 6h is a reasonable
  default. Their window slides continuously, so garbage accrues continuously,
  and `gc_interval` is also the amount of expired-but-not-yet-deleted blobs the
  disk is carrying.
- **A run that takes longer than `gc_interval`** means the configured cadence
  cannot be sustained. Runs are serialized rather than allowed to sweep the
  same store concurrently; watch the collector duration and progress signals.
- **Epoch memory is O(|M| + |T|).** M already existed in the pre-epoch collector;
  T adds one entry per distinct application-touched multihash, not per request.
  Watch process RSS beside `gc_protected_blocks` on a heavily read archive. Both
  sets are released at epoch end.
- **`store.scrub_interval` defaults to 168h (one week).** The first scrub starts
  after half that interval; with the default 24h GC cadence, the 84h offset
  keeps later weekly scrubs halfway between GC ticks. Serialization is still
  the backstop if either job overruns. Tune this for integrity-detection latency
  and full-store read I/O, independently of the amount of garbage to reclaim.

A failed collector run is logged and the schedule continues. GC is maintenance,
and a daemon that exited because a sweep failed would turn a disk-space problem
into an outage. A mark still has to read and validate DAG-CBOR blocks in order to
discover links; every DAG-CBOR target is CID-validated, although a direct pin is
not traversed. Raw targets get a local existence check and are marked by
multihash without a full-byte read, because their bytes do not determine
reachability. A missing target still fails a written head or takes the follower
heal path.

A prepare or mark failure performs no sweep. A sweep failure can leave a partial
reclamation, but every completed deletion already passed the same M ∪ T check;
the consequence is leftover garbage, not an unsafe half-commit. Epoch cleanup is
fail-safe and the next scheduled run starts again from a fresh cut.

That last distinction is important when reading a green GC result: **collector
success means reclamation was safe; it no longer means every retained byte was
hashed.** Full blockstore validation is a separately scheduled scrub. It
completely enumerates flatfs and validates every object it observes, including
unreachable garbage; a concurrent addition may wait for the next pass. It
reports its own completion, failures, object count, and validated
bytes. Size and alert on the scrub independently from GC; schedule it for an
I/O-appropriate period. It neither deletes nor refetches blocks.

GC and scrub are serialized against each other, so a scheduled pass may wait
for the other maintenance job; normal reads and writes continue.

On a **writer**, a marked block the GC cannot read or find is local divergence
and fails the mark closed before sweep. On a **follower**, a genuinely absent
marked block may be fetched back over the follower's bitswap path. Written heads
are checked before followed heads so a shared block cannot be laundered through
follower healing. A block that is present but corrupt is never silently
overwritten: DAG corruption encountered by mark fails immediately, while raw
and otherwise unreachable corruption is reported by scrub and repaired
with `bloard fsck --repair` (§7.8). Scrub itself never repairs it.

Expect a collector log to make its epoch/cut, mark, sweep, protection, and total
duration visible, and a scrub log to identify itself separately and report its
object and byte counts. Long mark/sweep and scrub loops check progress every
8,192 objects and emit/update it at most once per minute: mark logs the head,
marked count, and frontier; sweep updates scanned/protected gauges and logs
deletes/protected skips; scrub updates scanned/validated-byte gauges. A flat
progress signal is an I/O or implementation incident even if ordinary requests
remain healthy; a scrub failure is an integrity incident even if the most recent
collector succeeded.

Snapshot-stable HTTP reads are preserved across a concurrent root replacement:
an already-selected retired root keeps the next T0 behind its bounded
materialization, and a request admitted after T0 uses the active epoch's per-key
protection. This is deliberately a response-materialization lease, not a client
lease: the potentially slow body write happens after release.

### 5.2 Staging pins (`ingest.staging_ttl`)

Spec 7.2's ingest is two requests: `POST /bloar/v1/blobs`, then `POST
.../refs`. Between them a blob is stored and reachable from nothing, and a GC
landing in the gap used to sweep it (spec 9's window (a)).

It no longer does. Every blob a put accepts gets a direct pin under the reserved
`_staging` head (§2.3) before the put answers, so it is in the mark set from the
moment the indexer is told the put succeeded. The pin is dropped when the refs
naming it land, and expires on its own if they never do.

`ingest.staging_ttl` (default 24h) is that expiry. It bounds the cost of an
abandoned put -- an indexer that crashed between its two requests -- at
`max_put_blobs * 128 KiB` per abandoned batch for the TTL: **8 MiB per crash for
a day**, at the defaults.

Leave it alone. Lowering it gives a slow indexer less room to finish its batch;
raising it retains junk longer. If abandoned puts are costing you real disk, the
number to change is not this one -- something is crash-looping between two HTTP
requests, and `bloar_staging_expired_total` climbing is the daemon telling you
so. It is logged at WARN for the same reason.

---

## 6. Monitoring

Set `server.metrics_listen` (e.g. `127.0.0.1:9550`); empty is the default and
turns the whole thing off, building no registry at all. **Bind it privately.**
The API listener is public; the metrics, health, and readiness endpoints are not,
and must never share its interface (§6.3 is why the API listener is safe to expose
in the first place).

Three endpoints:

| Path | Answers |
|---|---|
| `/metrics` | Prometheus exposition. |
| `/healthz` | **Liveness**: the process is serving HTTP. Nothing else. |
| `/readyz` | **Readiness**: every gate met, else 503 + the unmet gates. |

`/healthz` deliberately checks nothing but itself. A liveness probe that
consulted the store is one that restarts the daemon when the disk is slow, which
turns a degraded node into a crash loop -- and the one thing that must not happen
to bloard is being killed while it holds Pebble's lock and a half-applied batch.
Readiness is the probe that has an opinion about whether the answers would be
right.

The `/readyz` gates are `store` (store open), `heads` (every configured WRITER
head registered), `reconcile` (first pin reconciliation done), `gc` (the periodic
GC scheduler launched and running -- withdrawn only if the scheduler stops with a
terminal error, NOT when one collection fails), and one separate
`followed_head:<name>` per configured FOLLOWED head. A followed-head gate means:
this process currently has that head registered and serviceable, having adopted
it from a verified publication document (resumed from the durable checkpoint or
first adopted). It is NOT lazy-DAG (verify: full) verification, freshness, writer
reachability, or last-poll success: an ordinary poll failure does **not** withdraw
it (a served head keeps serving its durable generation while the writer is
unreachable). A quarantine (§11.4) does, and within this process no later poll or
resume resurrects a quarantined head (a restart re-evaluates from the durable
checkpoint -- quarantine is process-lifetime state) -- so a node that served data
which does not verify leaves the balancer and stays out. `store`/`heads`/
`reconcile` are established once, `gc` regresses only on a terminal scheduler
failure, and a followed-head gate is the one that regresses in normal operation.
The scrub scheduler has no readiness gate: a failed integrity pass is an alert
and may require repair, but taking the service out of rotation without first
classifying whether the named object is live would conflate unreachable garbage
with a served-data fault. Use the scrub outcome and §7.8 rather than readiness.
Gate and metric labels are config-bounded (one series per configured head).

The metrics listener comes up before the things it measures and outlives them, so
`/readyz` is answerable (and correctly 503) during startup.

**The indexer has its own.** `bloar-index` (spec 10) is a separate process from
`bloard`, and it takes a top-level `metrics_listen` with the same semantics --
empty is off, bind it privately, and pick a port distinct from a co-located
archive's `server.metrics_listen`. It serves the same three endpoints. Its
`/readyz` has no gates: a stateless client establishes no readiness fact (nothing
about it is *wrong* rather than *slow* for a probe to gate on), so it answers
ready whenever the process is up -- the same thing `/healthz` already says. Over
the shared family it adds `upstream_read_duration_seconds` and
`upstream_read_bytes_total` (§6.1); the rest of the family scrapes as zero on an
indexer.

### 6.1 What is published

All under the `bloar_` namespace. Every label is bounded by the config: `head`
is your heads, `purpose` is pinning's six, `status` is an HTTP status *class*.
Nothing is labelled by slot, CID, versioned hash, or peer -- those are the four
unbounded dimensions bloar has, and one of them in a label would grow the
registry without limit for the life of the process. A read for an unknown head
is labelled `head="_unknown"`, because `{head}` is attacker-controlled on a
public API.

| Metric | Notes |
|---|---|
| `head_synced_to{head}` | The lag alert's input. **Absent** on an empty head -- see `head_covered`. |
| `head_covered{head}` | 1 if the head covers any slot. `synced_to` is meaningless without it: 0 is a slot. |
| `head_dir_depth{head}` | Directory depth (spec 3.3). Grows at exact capacity boundaries; a surprise here is a bug. |
| `head_root_swaps_total{head}` | Roots that became current. A writer's are mutations; a follower's are adoptions. |
| `head_adoptions_total{head}` | Follower only. |
| `head_quarantined{head}` | **1 is an emergency.** See §6.2. |
| `beacon_reads_total{head,status}` | `status` is `2xx`/`4xx`/`5xx`. 4xx is normal (404 = no such blob). |
| `beacon_read_duration_seconds{head}` | Nitro syncs one slot at a time, serially: this gates sync speed. |
| `public_read_admissions_total{outcome}` | Weighted public GET admission decisions. `outcome` is the fixed set `admitted`/`rejected_global`/`rejected_client`/`rejected_canceled`; no path or client labels. |
| `public_read_admission_cost_total{outcome}` | Request-cost units behind those decisions. Compare rejected cost with admitted cost when tuning provisional defaults. |
| `ingest_blobs_total`, `ingest_bytes_total` | |
| `ingest_rejects_total{reason}` | `framing`/`kzg`/`store`. A non-zero `kzg` means an indexer is sending junk. |
| `ingest_kzg_verify_duration_seconds` | The one hot path in ingest. Higher under `CGO_ENABLED=0` builds (§8). |
| `store_put_duration_seconds` | Time to write one blob's block to the flatfs blockstore in ingest. The blockstore only, not the catalog KV; rising here is an IO-bound ingest. |
| `pins{head,purpose}` | `head="_staging"` is the reserved head (§2.3). `purpose` is `root`/`index`/`window`/`open`/`manifest`; `manifest` is the recursive pin on a head's manifest chain tip (spec 10.5) and is 1 for a head with a chain, 0 otherwise. |
| `pins_added_total{head}`, `pins_removed_total{head}` | |
| `pin_reconcile_duration_seconds{head}`, `pin_reconcile_errors_total{head}` | |
| `gc_runs_total{outcome}`, `gc_active`, `gc_duration_seconds` | `outcome` is `ok`/`error`; `gc_active=1` means an online GC run is in flight (including prepare/census outside the open protection epoch). |
| `gc_phase_active{phase}`, `gc_phase_duration_seconds{phase}` | Bounded `phase` is `prepare`/`mark`/`sweep`/`census`. Use this to distinguish a short cut from the concurrent archive walks. |
| `gc_marked_blocks`, `gc_scanned_blocks`, `gc_swept_blocks_total` | Completed reachability count, in-flight/last enumeration count, and cumulative deletes. Online enumeration is not a point-in-time store census. |
| `gc_protected_blocks`, `gc_protected_skips_total` | In-flight/last distinct application-touched multihashes, and cumulative candidate deletions prevented by T. A nonzero skip is direct evidence that the protection boundary resolved a real race. |
| `gc_last_success_timestamp_seconds` | Stamped by every successful run. Watch it stop moving -- see §6.2. |
| `gc_refetched_blocks_total` | Follower only. Marked blocks fetched after a DAG read or raw/direct existence check found them missing (§5.1). The protection epoch closes ordinary fetch-overlap loss; any increase deserves investigation even though a successful heal is not an outage. |
| `scrub_runs_total{outcome}`, `scrub_active`, `scrub_duration_seconds` | Full validation of objects observed by one complete enumeration, separate from reclamation; scrub never deletes or refetches. |
| `scrub_scanned_blocks`, `scrub_validated_bytes`, `scrub_last_success_timestamp_seconds` | In-flight/last scrub counts and last successful completion. A GC success does not advance these. |
| `staging_pins`, `staging_expired_total` | Sampled at each GC run. |
| `bitswap_fetches_total{outcome}`, `bitswap_fetched_bytes_total` | |
| `p2p_reachability{state}`, `p2p_live_peers{direction,transport}` | One-hot AutoNAT state and live connected peers in closed transport cells. A relay connection is control-plane reachability, not successful direct data transfer. |
| `bitswap_scheduled_bytes_total{peer_class}` | Raw block payload placed into outbound Boxo envelopes, before network write. It is attempted/scheduled payload, **not** delivery acknowledgement or proof that a peer retained the block. |
| `rendezvous_active{operation}` | Local lifecycle/configuration state for `provide` and `discover`; it says nothing about network success. |
| `rendezvous_provides_total{outcome}`, `rendezvous_provide_last_success_timestamp_seconds` | Rendezvous namespace-key DHT writes and the last locally successful call. A successful RPC is not proof of remote propagation. |
| `rendezvous_discovery_rounds_total{outcome}` | Bounded local rounds: `available`, `empty`, or `timeout`. `available` means at least one candidate was connected by this node. |
| `rendezvous_observed_provider_samples` | Provider records consumed in the most recent bounded query. Never interpret this as global provider cardinality, honest membership, or even reachable peers. |
| `ipns_publication_stage_total{stage,outcome}` | The load-bearing publication transaction. `stage` is the closed ordered pair `provide_document` then `put_record`; a successful `put_record` therefore means the exact document CID was provided before IPNS named it. This is the publisher path, not the auxiliary exact-pointer hint service below. |
| `ipns_publication_last_success_timestamp_seconds` | Historical local completion time of the last full provider-before-IPNS transaction. It does not reset when a newer document becomes pending. Compare its age with `publish.ipns_republish` (4h by default); a successful local RPC is still not proof of remote propagation. |
| `edge_publication_stage_duration_seconds{stage,outcome}` | Split-edge DHT latency for `provide_document` and `put_record`, with closed outcomes `ok`/`error`/`timeout`/`canceled`. The edge transaction deadline produces `timeout`; this histogram is the tuning source for that deadline and is distinct from the writer's end-to-end counter above. |
| `pointer_current{kind}` | Whether the exact-pointer provider currently has a `root`, `manifest`, or verified publication `document`. In a split deployment this schedule is owned by the public edge, derived only from the signed document after its successful IPNS commit; the private writer intentionally exports zero-valued pointer series. No CID is a label. |
| `pointer_provides_total{kind,outcome}`, `pointer_retries_total{kind,reason}` | Exact-pointer DHT calls and locally scheduled retries. Retry reasons are the closed set `ineligible`, `check_error`, and `provide_error`. |
| `pointer_provide_last_success_timestamp_seconds{kind}` | Oldest successful local DHT write across **all current** pointers of this kind. It is zero until every current CID has succeeded. Partial withdrawal recomputes it from retained CIDs; complete withdrawal or an unprovided new/replaced CID resets it to zero, so an obsolete or partial schedule cannot claim freshness for the aggregate. |
| `pointer_schedule_updates_total{outcome}` | Bounded local handoffs from an authenticated edge publication to its exact-pointer schedule. `error` includes preflight rejection or a post-IPNS activation failure; the latter is logged and withdraws local hints rather than turning an already-successful primary transaction into a writer error. |
| `follow_polls_total{channel,outcome}` | `channel` is `https`/`ipns`, `outcome` is `ok`/`error`. One sample per configured channel per poll. `ok` judges the document, not its heads -- see the notes. |
| `follow_refusals_total{reason}` | Follower only. Adoption refusals. `reason` is `synced_to_floor`/`manifest_ancestry`/`coverage_mismatch`/`quarantined` (per-head), `updated_at_floor` (whole-document), or `ipns_seq_floor` (IPNS channel). Not the same event as a poll `error` -- see the note and §6.2. |
| `follow_synced_to_floor_lag{head}` | Follower only. Slots this node still serves as covered that the writer has retracted below its published `synced_to` (spec 11.3): the no-regression floor minus a publication's `synced_to` when it is refused on the floor, and 0 once one is accepted. Sustained nonzero = a truncate-and-re-sync divergence window -- see §6.2. |
| `follow_head_ready{head}` | Follower only. `1` means exactly: this process currently has that configured followed head registered and serviceable, having adopted it from a verified publication document (resumed from its durable checkpoint or first adopted); `0` before then. NOT lazy-DAG (verify: full) verification, freshness, writer reachability, or last-poll success. One config-bounded series per configured followed head (initialised to `0`). It is the per-head view of the `followed_head:<head>` gate: an ordinary poll failure does not lower it, but a quarantine (§11.4) returns it to `0` and no later poll or resume resurrects it in this process. A head stuck at `0` is one `/readyz` is holding the node out of the balancer for. |
| `follow_source_available{source}` | Source-set follower only. `1` when the latest serialized poll produced an authenticated document which passed source-local replay, handoff, and quarantine checks; `0` otherwise. This is publication-plane health, not whole-snapshot commit, raw transport, or Bitswap reachability. |
| `follow_source_last_success_timestamp_seconds{source}` | Source-set follower only. Local receipt time of the last admitted document. Zero before the first success; never derived from the writer-controlled document timestamp. |
| `follow_source_head_covered{head,source}`, `follow_source_head_synced_to{head,source}` | Latest successfully observed claim for an authorized source/head cell. `synced_to` is absent when uncovered. Last claim values remain across an outage and must be interpreted with source availability or success age. |
| `follow_source_selected{head,source}` | Source-set follower only. One-hot source provenance of the durable last-good selected checkpoint. It changes only after commit and persists across writer outages, quarantine, and restart; pair it with `follow_head_ready` for current serviceability. It is not block-provider attribution. |
| `follow_conflict_active{head}`, `follow_conflicts_total{head,source}` | Source-set finalized heads. Durable hard-conflict latch and newly created bounded evidence participation. An active latch freezes advancement while the last-good generation remains served; see [Independent writers](multi-writer.md#9-investigating-and-clearing-a-conflict-latch). |
| `follow_incomparable_active{head}`, `follow_incomparable_total{head}` | Source-set finalized heads. Retryable root/manifest partial order which currently prevents a safe winner, and total observations. Unlike a conflict, it is not durable evidence and clears after claims become comparable. |
| `store_blocks` | Objects observed remaining by the last online sweep. Concurrent additions may not be enumerated until the next run, so this is a trend signal rather than an exact point-in-time census. |
| `store_kv_entries{prefix}`, `store_kv_bytes` | Pebble key counts by bounded prefix and its live on-disk bytes. Watch the filesystem for authoritative total store bytes. |
| `upstream_read_duration_seconds` | **Indexer.** Time to fetch one slot's blobs from the upstream (spec 10.1), retries included. Every answered fetch, blobs or not. |
| `upstream_read_bytes_total` | **Indexer.** Blob bytes read from the upstream. The throughput a perf drill had to hand-curl before this existed. |

Plus the standard Go runtime and process collectors. An enabled embedded host
also registers the pinned go-libp2p relay-service, AutoRelay, and hole-punch
collectors on this same private registry. Their names intentionally retain the
upstream `libp2p_relaysvc_`, `libp2p_autorelay_`, and `libp2p_holepunch_`
namespaces; Bloar does not copy or relabel them. See
[Swarm monitoring](swarm-monitoring.md) for exact panels, alert expressions,
and the boundary between discovery, relay control-plane, direct upgrade, and
Bitswap data-plane evidence.

> **On `follow_polls_total`:** it is recorded where the follower judges what a
> channel gave it, not at the HTTP client, so `outcome="error"` means the poll
> genuinely failed -- an unreachable writer, a bad status, a document that does
> not decode, is for another network, is signed by a key this node does not
> follow, or is a replay of an older one. A document that arrives with a 200 and
> then fails its signature check is an `error`. The log line says which; the
> counter only says that it happened.
>
> A follower on both channels contributes one sample to each per poll, and one
> channel failing while the other succeeds is normal and not worth alerting on
> -- that redundancy is the point of spec 8.1. Alert on **both** channels failing
> together, or on `head_synced_to` lag, which is the outcome that actually
> matters.

> **On `follow_refusals_total`:** a poll's `outcome="ok"` is a judgement about the
> *document* at resolution time -- it decoded, was for this network, verified against
> the followed key, and was fresh (both resolvers reject a document older than the
> freshness floor before the poll is counted, so an authentic-but-replayed resolution
> is an `error`, not an `ok`). A document that clears that bar can still be refused
> when it is admitted -- a concurrent poll may have moved the floors since, or a head
> of it may be inconsistent -- and those refusals are counted here, not as a poll
> `error`. They act at three levels.
> **Per-head** (refusing one head of a document): `synced_to_floor` is a published
> head whose coverage is below what this node already served -- a writer that
> regressed, or someone replaying an old archive document at your follower;
> `manifest_ancestry` is a published manifest tip that does not descend from the one
> this node already accepted (spec 10.5, 11.3), a rewritten filter history;
> `coverage_mismatch` is a head whose root's derived coverage disagrees with the
> `synced_to` it claims; `quarantined` is a head this node stopped
> serving for failing verification (spec 11.4) refusing re-adoption. **Whole-document:**
> `updated_at_floor` is a document dated before the freshness floor a concurrent poll
> raised -- the entire document is refused, no head of it adopted. **IPNS channel:**
> `ipns_seq_floor` is a record whose sequence a newer poll's record already lifted the
> replay floor past, a replay refused on the number alone. A regressed writer therefore
> shows up as `ok` polls and a rising `synced_to_floor` -- which is exactly the split
> that made a run of real refusals invisible before this counter existed.

### 6.2 Alerts worth having

**`synced_to` lag.** The signal that the archive is falling behind. Compare
against wall-clock slot rather than against nothing:

```
(time() - 1606824023) / 12 - bloar_head_synced_to > 300
```

~1 hour behind. Tune per head: an `arbitrum-one` head with `fetch_blobs: false`
trails the `all` head by design, so its threshold is looser. Gate on
`bloar_head_covered == 1`, or a fresh empty head pages you at 8.6 million.

**Quarantine.** Page on this.

```
bloar_head_quarantined > 0
```

It means this node caught the writer it follows serving data that does not verify
(spec 11.4) -- under `verify: full`, a blob whose KZG commitment does not match
the versioned hash the index claimed. That is evidence of a malicious or broken
writer, not a transient. The head stops being served, and it is one-way for the
life of the process: nothing clears it but an operator deciding what happened.

One quarantined head **freezes adoption of every other head that writer
publishes.** A follower admits a publication document as a whole, so once any head
in it is quarantined, every subsequent document is refused (`reason="quarantined"`)
and none of the writer's other heads advance either -- the healthy heads go on
serving what they last adopted, but they stop taking updates. This is deliberate:
a writer whose signature vouched for a head that then served a forged versioned
hash has forfeited the trust the same signature places in its other heads, so the
node holds the whole document until an operator investigates. Restarting the
follower clears the in-memory quarantine and lets adoption resume -- do that only
once you have decided the writer is sound (a genuine writer bug now fixed, not a
compromise); if the bad blob is still published, the next `verify: full` read
re-quarantines it.

**Follower refusals.** Any nonzero rate is worth a look:

```
increase(bloar_follow_refusals_total[1h]) > 0
```

`reason="synced_to_floor"` is the one to care about: the writer you follow
regressed a head, or someone is replaying an old archive document at your
follower. Either way this node refused to take its coverage back -- correctly --
and someone should find out why the writer went backwards. A regressed writer is
invisible in `follow_polls_total`, whose `ok` is about the document and not its
heads, which is the whole reason this counter exists. `reason="quarantined"` is a
head already quarantined (page on that above) whose writer is still publishing
it; the refusal is the node holding the line every poll. `reason="manifest_ancestry"`
means the writer published a filter history that discards the chain this node
holds: either the writer minted a new manifest chain (a bug, or a forced recovery
done in the wrong order -- see §7.5), or someone is feeding your follower a forged
document. The node refuses it and keeps attesting the chain it already has.

**Follower divergence window.** The refusals above are events; this is the state
they leave behind. When a `synced_to_floor` refusal fires during a legitimate deep
truncate-and-re-sync, the follower goes on serving its last good root for the
slots between the writer's new lower `synced_to` and the floor it already served,
and answers those slots differently from the writer until the writer's coverage
climbs back past the floor (spec 11.3's bounded, self-healing tradeoff).

```
bloar_follow_synced_to_floor_lag > 0
```

A nonzero value is expected during a deep truncate and clears itself once the
writer re-passes the floor; the gauge resets to 0 the moment a publication is
accepted. Investigate a value that persists after the writer should have caught
back up -- a writer that never re-covers those slots leaves the window open, and
the two nodes disagree on that range for as long as it stays open.

**GC failures.**

```
increase(bloar_gc_runs_total{outcome="error"}[1d]) > 0
```

Investigate rather than restart. Failures in `prepare` are reconciliation,
staging-expiry, or pin-snapshot failures; `mark` failures are missing live data,
an undecodable/invalid DAG-CBOR node, or a failed follower heal; `sweep` failures
are enumeration or deletion I/O. `census` is post-sweep accounting, not another
reclamation pass.
A raw target which is absent still fails or heals in mark, but corruption in an
otherwise present raw object is a scrub finding because GC uses an existence
check there. Present corruption is never overwritten as a follower
heal. Use `bloard fsck --repair` under §7.8 rather than restarting blindly.

**GC not succeeding.** The counter above catches a run that fails loudly; this
catches one that has quietly stopped running or stopped finishing. Alarm when no
run has succeeded in `3x gc_interval`:

```
(
  bloar_gc_last_success_timestamp_seconds > 0
  and time() - bloar_gc_last_success_timestamp_seconds > 3 * <gc_interval seconds>
)
or
(
  bloar_gc_last_success_timestamp_seconds == 0
  and time() - process_start_time_seconds > 3 * <gc_interval seconds>
)
```

The startup arm matters: the last-success gauge is zero until the first scheduled
run, which occurs after one interval, and zero must not page immediately as a
Unix-epoch timestamp.

When `bloar_gc_active == 1`, `bloar_gc_phase_active` says whether the run is in
`prepare`, `mark`, `sweep`, or `census`. The two archive-walk phases can be long;
`prepare` should remain a short publication barrier. Page on a sustained prepare
phase because that is write-path unavailability. A long mark or sweep is an I/O
capacity incident only when it stops making progress or threatens the cadence.

**Integrity scrub failures or staleness.** GC success is not a substitute for
this alert:

```
increase(bloar_scrub_runs_total{outcome="error"}[1d]) > 0

(
  bloar_scrub_last_success_timestamp_seconds > 0
  and time() - bloar_scrub_last_success_timestamp_seconds > 3 * <store.scrub_interval seconds>
)
or
(
  bloar_scrub_last_success_timestamp_seconds == 0
  and time() - process_start_time_seconds > 3 * <store.scrub_interval seconds>
)
```

The same startup guard applies here. The initial scrub is offset by half an
interval, so an unset last-success gauge is normal during that first half-window.

`bloar_scrub_active` distinguishes a long live pass from a scheduler which has
stopped. The last completed pass publishes `bloar_scrub_scanned_blocks` and
`bloar_scrub_validated_bytes`; compare them with prior runs and filesystem
growth. A failed scrub may name corrupt unreachable garbage as well as live
data. It deliberately does not delete or refetch either class: classify the CID
and use §7.8.

**Dangling pins on a follower.** A follower's GC heals a marked block the store
is missing by fetching it back rather than failing (§5.1), so a successful heal
is not an outage. The online collector now protects both fetched blocks and
blocks found already present during an epoch, scopes subtree-walk memos to the
monotonic collection generation, and gates final adoption. A retention walk
which crosses a generation leaves its completion markers unchanged and stale
and its staging pins intact for retry; GC overlap with an ordinary fetch pass is
no longer an expected source of dangling pins. A refetch can represent legacy
damage or independent local loss. Any sustained rate deserves investigation:

```
rate(bloar_gc_refetched_blocks_total[6h]) > 1/3600
```

Investigate rather than page: the head is being served throughout, and each heal
is a block put back.

**GC not keeping up.** A run longer than `gc_interval` means the serialized
scheduler cannot maintain the requested cadence (it does not run two collectors
against one store concurrently):

```
histogram_quantile(0.9, rate(bloar_gc_duration_seconds_bucket[7d])) > <gc_interval seconds>
```

**Abandoned ingests.**

```
increase(bloar_staging_expired_total[1d]) > 0
```

Every one of these is a blob an indexer was told it had stored and then never
referenced (§5.2). Not urgent; a symptom.

**Readiness.** `/readyz` in the load balancer's health check. That is what it is
for.

### 6.3 The API listener's own bounds

The public API listener is **safe to expose directly.** Put a CDN or reverse proxy
in front of it for caching (the immutable window, `server.immutable_horizon_slots`,
is what makes a CDN pay for itself) and for another layer of rate and connection
limiting -- that is defense in depth, and worth doing -- but the listener does not
*depend* on one to be safe. Its bounds are its own, in the code, with defaults that
hold on a listener bound to `0.0.0.0`.

The `server.*` keys that set them, with their shipped defaults, are written out in
[`deploy/examples/writer.yaml`](../deploy/examples/writer.yaml). In short:

- **`read_timeout`** (15s) bounds the wall-clock to read a *whole* request, header
  and body. It is the load-bearing one: a request that never finishes sending its
  body -- including one the server rejects before reading (a bad token, an unknown
  head, a malformed frame), where the kernel and net/http would otherwise drain the
  body to close the connection -- is bounded here rather than able to pin a
  connection. `read_header_timeout` (10s) is the header-only bound underneath it.
- **`mutation_body_timeout`** (60s, and it must exceed `read_timeout`) is the
  read-deadline a *valid* authenticated upload gets once it is past its auth, head,
  and framing checks, so a legitimate multi-megabyte POST over a slow-but-honest
  link is not caught by the short base bound.
- **`write_timeout`** (120s) caps how long the blobs endpoint may spend writing one
  response, so a slow reader of a maximum multi-megabyte body cannot hold a handler
  -- and the response-memory reservation it took -- open forever.
- **`idle_timeout`** (60s) closes a kept-alive but silent connection.
- **`max_conns`** (1024) caps concurrently open connections; **`max_header_bytes`**
  (64 KiB) caps request headers.

The default-on **`public_read_admission`** block bounds how quickly those
already-bounded requests enter the server. A metadata GET costs 1 unit. A
filtered blobs GET costs `1 + len(versioned_hashes)` after repeated-key or
comma-separated array values are expanded, duplicates included. An unfiltered
blobs GET is charged conservatively at `1 + max_query_hashes` before any block
lookup. The process-wide bucket is authoritative; a bounded TTL/LRU adds
per-client fairness without letting address churn bypass the global rate.
Rejection is a non-cacheable `429` with an integer `Retry-After`, and rejected
reservations consume neither bucket.

The shipped rates are **provisional pending rollout load evidence**: 4096 units/s
and 16384 burst globally; 1024 units/s and 4096 burst per client; 4096 client
buckets with a 15-minute TTL. At the default maximum request cost (129) that is
about 8 sustained maximum-cost reads/s and 31 in a burst for one client;
filtered serial sync is normally much cheaper. Tune against
`public_read_admissions_total`, `public_read_admission_cost_total`, response
latency, and the target host. `enabled: false` is the explicit opt-out; zero
selects a default and does not disable a bound.

Before changing the provisional rates, replay a long weighted serial trace at
the proposed limits and confirm both that admitted traffic receives no
accidental `429` and that limiter bookkeeping has ample CPU headroom. That test
validates the limiter configuration, not the absolute rates: target-host
storage, rendering, network, and competing-load measurements are still required
before removing the provisional label.

Client identity is the socket peer. Forwarding headers are ignored unless
`trusted_proxy_header` and `trusted_proxy_cidrs` are both set. CIDRs must be
canonical native IPv4 or IPv6 network prefixes (host bits, duplicates, zones,
mapped IPv4 and whitespace are rejected). The rightmost untrusted address in a
valid appended chain becomes the client; IPv6 clients are aggregated to /64.

Setting a key to **zero does not disable its bound** -- config loading and the
server both replace a zero with the safe default above, so a bound is always in
force. To loosen one, set a larger value, not zero; a negative value is a startup
error. If a proxy in front already enforces tighter limits, these stay as the
backstop for the day the proxy is bypassed or misconfigured.

### 6.4 Serving a finalized-plus-live view

`live_heads` is an opt-in, local routing layer for clients which want finalized
history and the bounded optimistic tip through one beacon URL. It does not mint,
sign, publish, pin, or mutate a head of its own:

```yaml
live_heads:
  live:
    finalized_head: all
    unfinalized_head: unfinalized
```

Both referenced names must already be declared under `heads` or `follow.heads`;
the first must be `finalized-monotonic`, the second `unfinalized-mutable`, and
`live` must not collide with a physical name. For a locally written mutable
head, `unfinalized.handoff_head` must be this exact `finalized_head`; startup
rejects a cross-wired view rather than serving a handoff the writer did not
prove. For followed heads, the signed entry authenticates kind and window and
also carries the writer's exact finalized handoff name, root, and frontier.

The ordinary follower selects that same finalized head and mutable head. A
chain-filtered replica may instead retain its filtered finalized archive plus
the global bounded mutable tip, without retaining the global finalized archive:

```yaml
follow:
  # url/pubkey/p2p settings omitted here
  heads:
    arbitrum-one:
      pin: { mode: full }
    unfinalized:
      kind: unfinalized-mutable
      handoff_head: all             # signed global proof authority; metadata only
      max_window_slots: 64
      pin: { mode: full }           # required; the tip is bounded

live_heads:
  arbitrum-one-live:
    finalized_head: arbitrum-one    # retained filtered frontier
    unfinalized_head: unfinalized   # retained global bounded tip
    require_versioned_hashes: true
```

The same physical selection works with independent publication authorities:
configure the bounded `follow.sources` roster and its acknowledged generation
as described in [Independent writers](multi-writer.md#follower-source-set-configuration).
Only finalized heads are arbitrated across sources; the mutable head must occur
in exactly one source's allowed-head list.

Treat `follow.source_set.revision` as a durable monotonic authorization floor.
If daemon startup fails after accepting a new roster, rollback is a roll-forward
operation: restore the former roster under a still higher revision and its
recomputed acknowledgement digest. An older revision will correctly be refused;
startup activation by itself does not admit a source document or replace a
served head.

Here the signed `all` line is still required in every mutable proof, but it is
metadata only on this replica: it has no independent selected checkpoint and is
not fetched, pinned, served, or republished. Its exact line is nested in the
mutable checkpoint so restart can re-establish the same proof without selecting
`all`. `arbitrum-one` and `unfinalized` are the only physical selections.
The follower admits them as one authenticated transaction and blocks the whole
document if `unfinalized.window_start` is more than one slot above
`arbitrum-one.synced_to`. This refusal increments
`bloar_follow_refusals_total{reason="handoff_blocked"}` and leaves the old
checkpoints, serving snapshot, pins, and replay floor intact. It is safe during
GC: pointer publication and local retention change atomically, and an external
store drains readers of the old snapshot before retiring its anchor.

If `arbitrum-one` is written locally and only `unfinalized` is followed, it
cannot be part of that remote document transaction. This mixed role is still
safe: both mutations share the registry/GC gate and the live selector returns a
retryable `503` for any boundary gap. It does not reject the remote publication
as `handoff_blocked`; monitor the physical head frontiers as well.

Point an opt-in client at `http://archive:8550/arbitrum-one-live`. Finalized slots
remain enumerable. Provisional requests on this filtered view must name at least
one `versioned_hashes` value; a no-hash provisional request is a no-store `400`.
The hashes are exact lookups rather than an ACL, so a known foreign-chain hash in
the global tip is still retrievable. That is intentional: the live tip is small,
complete, and shared. Foreign blobs disappear when their mutable generation
rotates out unless the filtered finalized archive also references the same
content-addressed block. Existing clients on `/all` are unaffected, and the
alias is deliberately absent from `/bloar/v1/heads` because it is local serving
policy rather than an independently authenticated root.

The response header is the quick operational proof of which side answered:

- `X-Bloar-Finality: finalized` means the slot was at or below this view's
  selected finalized-head frontier. Presence and 404 absence both come only
  from that finalized head and retain its normal cache policy.
- `X-Bloar-Finality: provisional` means the slot came from the complete mutable
  generation. These responses are always `Cache-Control: no-store` because a
  reorg can change presence or absence at the same slot.
- `503` + `Retry-After: 12` + `no-store` means the virtual view could not prove a
  coherent owner for that slot: first-start before a mutable generation, a
  coverage gap, quarantine, finalized rewrite/restart mismatch, source-finality
  watermark overrun, or a request beyond both tips. Do not configure a proxy to
  turn this into a 404. The physical mutable path uses the same retryable 503
  while its handoff proof is incoherent; it does not pretend the head is unknown.

The writer persists two finalized anchors for each mutable generation: what the
tracker observed, and what was current when selection committed. During one
process lifetime, ordinary finalized appends may advance the signed pair only up
to the source-finalized watermark. A real truncate invalidates the mutable side;
after restart, any root/frontier mismatch also stays fail-closed. Recovery is a
new tracker generation, not a manual root edit. This conservatism prevents a
truncate-and-reappend root ABA from reactivating an old optimistic snapshot.

All local root transitions are copy-on-write. Operators should wire every writer
head through the reconciler replacement callback (the daemon does this
automatically): candidate DAG and signed document first, durable selector next,
then an infallible reconciler/registry/publication swap. Do not embed `Heads`
behind a collecting gate without the corresponding replacement callback.

The rollback is only a client URL change from `/live` back to `/all` (or removal
of the `live_heads` block followed by a daemon restart). Physical heads and
their publication remain unchanged either way.

---

## 7. Runbooks

### 7.1 Setting up a follower

Start from [`deploy/examples/follower.yaml`](../deploy/examples/follower.yaml).

1. **Choose the signer trust root.** For direct URL/IPNS, get the writer's hex
   ed25519 publication key out-of-band and put it in `follow.pubkey`. For a
   DNSLink trust root, configure `follow.dnslink`; omit `pubkey` only if you want
   an admitted DNSLink/IPNS chain to bootstrap and rotate the signer. Set both
   for the stronger pinned-signer posture.

2. **Get channels.** `follow.url` is the HTTPS data channel. Choose at most one
   name authority: direct `follow.ipns`, or `follow.dnslink` whose single TXT
   target is exactly `/ipns/<name>`. URL may accompany either; the freshest
   document that authenticates wins. The default DNS resolver does not validate
   DNSSEC, so use a signer pin when DNS control alone is not an adequate root.

3. **Get a peer.** `p2p.peers` must contain a multiaddr for the writer,
   including its PeerID. **This is the one people miss.** Peering is static in
   v1 -- the DHT carries IPNS records and is not consulted for block exchange
   (spec 11.2) -- so a follower with no reachable peer here fetches nothing, no
   matter how good the rest of the config is. The writer's own
   `p2p.announce` is what it publishes; the document's `multiaddrs` are dialled
   too, but do not rely on that alone.

4. **Choose a pin policy per head.** Required, with no default, deliberately:
   the writer's `full` would silently retain the entire archive on a node that
   asked to replicate one chain.

   | Policy | Holds |
   |---|---|
   | `{mode: full}` | Everything. A mirror. |
   | `{mode: window, duration: 720h}` | The whole index + 30 days of blobs. The usual answer. |
   | `{mode: none}` | The whole index, no blobs. |

   Every policy retains the **whole index** (spec 9). That is what lets a
   follower answer 404-vs-503 exactly like the writer -- "was there a blob at
   this slot" is answerable for all of history -- while holding almost no blobs.

5. **Start it.** It resumes anything it has already adopted from disk before it
   listens, then polls. Watch: `bloar_head_adoptions_total` climbing (it is
   seeing documents), `bloar_head_synced_to` catching up (it is fetching),
   `bloar_bitswap_fetches_total{outcome="error"}` flat (it has a working peer).

6. **Verify it.** Point Nitro at `http://follower:8550/<head>`. Passing that is
   the whole point (spec 13.8); the follower serves the read API identically.

**Backfill is not instant.** The first fetch pass walks the whole DAG for the
policy. Reads for blocks it has not got yet answer 503 + `Retry-After: 12`, which
is honest and retryable (spec 11.4).

### 7.2 Writer failover (promotion)

Spec 11.5. The situation: the writer is gone, and a follower should become it.

This works because the follower already holds the blocks AND the authoritative `f`
checkpoint that selects which root to serve (spec 11.3): promotion reads that
non-DAG checkpoint, materializes the durable `h`/`m` selectors from it, and retires
it in one atomic handoff before opening the head (`follow.ReconcileWriterPromotion`),
so the promoted writer resumes exactly the generation the follower last adopted.
Archive state does not live entirely in the DAG -- which root and which manifest tip
are current is precisely the non-DAG state promotion depends on. What the follower
lacks is only the right to say so: the signing key.

1. **Choose the authority model and isolate writer state.** For a legacy v1/v2
   takeover, stop the old writer before reusing its authority. Never run two
   processes against one writer store or copy one signer's private key into two
   independently advancing revision stores. A v3 replacement may coexist with
   the old writer only when it is genuinely independent: separate store,
   signing key, revision state, URL/IPNS name, and an operator-authorized source,
   with the same `archive_id` and logical head parameters.

2. **Provision the signing authority.** A legacy continuity takeover uses the
   old signing key (same file and permissions, §3.2). A v3 independent
   replacement should use a distinct key which followers authorize before the
   switch, while retaining the same `publish.archive_id`. Copying an old v3 key
   without its signer-local publication revision state causes replay; restoring
   that authority requires the complete writer state, not just its private key.

3. **Rewrite the config.** Delete the `follow:` block; add a `heads:` block. The
   head parameters **must match what the head was built with** -- `origin_slot`,
   `seg_bits`, `fanout_bits` are immutable for the life of a head (spec 3.1) and
   the daemon refuses to start on a mismatch rather than forking your archive.
   Read them off the publication document, or `GET /bloar/v1/heads/<head>`.

   Set `pin: {mode: full}` unless you mean otherwise. A follower's window policy
   carried into a writer role is a writer that garbage-collects its own history.

   Add `publish.signing_key_file`, the unchanged `publish.archive_id` for v3,
   and `publish.ipns: true` if this writer publishes there.

4. **Restart.** It resumes each head from the root it last adopted -- already on
   disk, already pinned.

5. **Point the indexers at it.** Change `archive.url` and restart them. They
   resume from the head's `synced_to`, which is what statelessness buys.

6. **Tell the followers.** Two different keys decide this, and only one is the
   signing key from step 2. IPNS followers resolve a name derived from the writer's
   **p2p identity** (`p2p.identity_key_file`, spec 8.1, §3.3), NOT the document
   signing key -- so keeping the same signing key does not keep the same IPNS name.
   A promoted follower keeps its own p2p identity, hence a *different* IPNS name,
   unless you also move the old writer's `p2p.key` onto it. So either:
   - **restore the old writer's p2p identity** onto the promoted node. That
     preserves ONLY the PeerID and the IPNS *name*, NOT the sequence state: the
     promoted node is a former follower with no writer `i`, and there is no tool that
     transfers the old writer's sequence onto it. A same-name republish therefore
     still needs `i` to reach at least the last sequence the old writer published --
     and it starts from zero with no deterministic catch-up (§4.6), so a higher extant
     DHT record keeps winning until then and you are back on the new-name path below; or
   - **accept a new IPNS name** and do the full name-rotation recovery: repoint
     consumers, with HTTPS as the interim (§4.6).

   Non-IPNS consumers: `follow.url` followers need the new URL. DNSLink followers
   need no config change: update the DNS record to the new `/ipns/<name>`. Unpinned
   followers may admit the replacement document signer; pinned followers require the
   same signer until their `follow.pubkey` is deliberately updated. A v3
   multi-writer follower instead authorizes the replacement source/key explicitly
   and keeps the same logical archive ID.

**The gap.** A promoted follower's `synced_to` is wherever the old writer got to
before it died. The indexers re-derive the rest. If the follower was on a
`window` policy it does not hold the blobs outside that window -- it still has
the whole index, so it answers correctly about them, but it cannot serve their
bytes. Backfill from another archive (§7.4) or accept the hole knowingly.

The daemon's promotion preflight draws exactly that line. Before it opens a
promoted head, it verifies -- offline, against the head's own retention policy --
that the whole index and manifest chain are present and hash to their CIDs, and
that every blob the policy retains is present. A blob the policy does **not**
retain (an out-of-window hole) is allowed: that is the "accept the hole
knowingly" case above, and it does not stop the promotion. But a missing or
corrupt index or manifest block, or a missing retained blob, is an unusable
generation -- the daemon refuses to start rather than mutate its stores or open
the head, so a promotion never half-completes.

### 7.3 Rebuilding node-local state

`bloard rebuild` reconstructs the blob catalog (spec 6.1) by walking every block
in the store. Run it with the daemon **stopped**: it wants the store's lock.

```
systemctl stop bloard
sudo -u bloar bloard rebuild -config /etc/bloar/bloard.yaml
systemctl start bloard
```

rebuild consumes no auth, so it loads the installed config even though its
`server.auth_token_file` is the systemd-credential form and there is no credential
directory here (§3.1): token resolution is deferred to the daemon's own token
read, which rebuild never reaches. Running it as `bloar` is fine for the same
reason -- it never opens the `root:root` token source.

`-clear` deletes every catalog entry before the walk, so the catalog ends up
saying nothing but what the blockstore holds. Use it when you suspect the catalog
of being *wrong* rather than merely incomplete; without it, a stale entry
pointing at a swept block survives.

When to reach for it:

- After restoring `blocks/` without the KV (§4.6).
- After a `409 missing_blobs` you cannot explain: the refs POST checks the
  blockstore, not just the catalog, so a 409 means the block really is not there
  -- but a catalog that has drifted is worth ruling out.
- Never routinely. It is a full walk of every block.

It does **not** touch the pin ledger, and must not: the ledger is rebuilt by
reconciliation instead, and the staging rows (§5.2) are the part of it that
reconciliation does not own. A rebuild that dropped them would unpin every blob
put but not yet referenced.

The **pin ledger** needs no command. It reconciles at startup, after every root
swap, and on a timer; GC reconciles before it marks. To force it, restart the
daemon.

### 7.4 Deterministic replication, and auditing an archive you don't run

These are two different jobs that a naive reading conflates. **Deterministic
replication** reproduces an archive's head byte-for-byte and proves the
reproduction was faithful. **Auditing** asks whether that archive is *complete* --
whether it dropped a real blob -- and no amount of replication answers it, because
a replica with no independent block feed copies the source's decisions (spec 11.5).
Do the one you actually want.

**Full archive availability through an existing Kubo node.** Run the
[standalone Kubo archive replica](kubo-replica.md) when the goal is to retain
and serve the complete authenticated DAG over Bitswap without a second IPFS
repository. It follows signed publications and refuses regressions, but runs no
writer, indexer, or ingest endpoint. Its optional, default-off read-only gateway
can serve the ordinary Bloar/beacon GET surface directly from the committed
Kubo-local generation, so a Kubo replica can be a Nitro beacon URL without a
second archive store. Public misses never trigger Bitswap. Kubo keeps ownership
of its Peer ID, repository, unrelated pins, and GC. This is deterministic
replication of the writer's decisions, not the independent completeness audit
described below. The linked operator guide covers the required Kubo 0.42
providing policy, first sync, monitoring, migration, and rollback.

**Decentralized swarm health.** Use
[`bloar-swarm-inspect`](swarm-census.md) to discover rendezvous providers through
the ordinary IPFS DHT and issue peer-targeted current and historical Bitswap
challenges. Its output is a timestamped lower bound from one observer, not an
exact global replica count; the linked guide covers privacy, bounded work,
Prometheus output, and the independent public-vantage rollout check.

**Deterministic replication (mirror mode).** Point a beacon indexer at an existing
archive's read API as its upstream, with `upstream.head` set to name the head --
the read API *is* the backfill protocol -- and let it build its own head from
scratch. KZG verification at ingest is inherent, so every included blob is checked
against its versioned hash on the way in. **The resulting head root MUST equal the
source's root** for the same parameters; if it does not, one of the two disagrees,
and CIDs being what they are you can walk down the DAG to find where. What this
proves is faithful reproduction of the source's coverage decisions, and that every
blob it *served* is KZG-valid -- **not** that the source omitted no real blob. A
covered-empty answer the source published over a dropped blob is reproduced here,
same root and all. Completeness is inherited from the source, not checked.

Use [`deploy/examples/mirror.yaml`](../deploy/examples/mirror.yaml). Setting
`upstream.head` is what selects mirror mode (and it is required: it is how the
indexer knows the upstream is an archive, so that an *in-range* 404 -- one at or
above the head's origin, which the startup origin check guarantees is the only
kind the walk ever queries -- is a protocol violation, not absence). A genuine
below-origin 404 is perfectly valid; the origin check is exactly what stops the
walk from querying one. An archive-to-archive read negotiates the raw
`application/octet-stream` variant (spec 7.1) automatically, halving the wire bytes
of a beacon-node backfill's hex; no configuration.

**Auditing a bloar archive you don't run (the honesty check).** The rule is: do
not let the archive be its own existence authority. Take existence from an
independent beacon node's block feed (it has every block, never pruned) and use
the archive only for bytes -- or not at all. Two runnable shapes test different
things, and the difference is real because a fallback is only tried *after* the
primary answers (main.go source order; the first anchored source returns), so an
archive placed behind a beacon-node primary is never asked about a slot the node
can serve.

**Pin the comparison to a captured boundary.** A live archive and your re-derivation
both keep advancing, and the indexer has no max-slot setting, so two *current* roots
can differ for the innocent reason that they sit at different `synced_to`. A root
comparison is only meaningful at a fixed boundary both sides reach exactly:

1. Capture ONE consistent snapshot of the audited head. `net` is a document-level
   field, so read it from the FULL publication document (`GET /bloar/v1/heads`); read
   the head's `{name, root, origin_slot, seg_bits, fanout_bits, synced_to = S}` -- and,
   for a filtered head, its `manifest` tip -- from the head entry (`GET
   /bloar/v1/heads/<name>`; the per-head entry carries no `net` and no decoded
   schedule). For a filtered head, also fetch the schedule with
   `GET /bloar/v1/heads/<name>/manifest` and require its returned `cid` to equal the
   captured entry's `manifest`. If the writer advanced between reads (the entry's
   `synced_to` or `manifest` moved), recapture, so the whole tuple is one point.
2. Build a fresh LOCAL audit head with the SAME name, network, and immutable params
   (`origin_slot`, `seg_bits`, `fanout_bits`) -- and, for a filtered head, the same
   manifest schedule -- by one of the two shapes below, re-deriving through at least
   `S`.
3. STOP the indexer feeding the local head, then -- if it overshot `S` -- truncate
   the local audit head back to `S` with an authenticated admin call (spec 5.4, 7.2),
   and require it to land at `S` before comparing:
   ```bash
   set -euo pipefail
   S=8700000                              # the captured boundary slot (numeric)
   HEAD=all                               # your LOCAL audit head name
   LOCAL=http://127.0.0.1:8550            # your local archive (server.listen)
   TOKEN=$(cat /etc/bloar/token)          # server.auth_token_file

   systemctl stop bloar-index@beacon-all  # the indexer filling this head

   # Truncate to S. %d renders the numeric slot into JSON safely; confirm must equal
   # the head's own name (spec 7.2). Bearer auth + JSON content type are required.
   curl -sf --connect-timeout 5 --max-time 30 -X POST "$LOCAL/bloar/v1/heads/$HEAD/truncate" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d "$(printf '{"slot": %d, "confirm": "%s"}' "$S" "$HEAD")"

   # Require the local head to be exactly at S before the root comparison.
   got=$(curl -sf --connect-timeout 5 --max-time 15 "$LOCAL/bloar/v1/heads/$HEAD" | jq -r .synced_to)
   [ "$got" = "$S" ] || { echo "truncate did not land at S ($got != $S)" >&2; exit 1; }
   ```
4. THEN compare the local root at `S` to the captured `root`. Equal roots mean the
   two histories agree up to `S`. A mismatch, given those preconditions, means they
   genuinely DISAGREE -- an omission, extra/wrong data, or a derivation defect -- and
   walking the DAG down from the two roots identifies which and where.

- **(ii) Byte-availability AND completeness, using the archive's own bytes.** Make
  the archive the anchored PRIMARY byte source, with an independent beacon node as
  the existence authority. Every slot's block-attested blobs are demanded from the
  archive, so a blob it dropped or cannot serve fails the batch at that slot.
  Because setting `upstream.head` would select mirror mode (and forbid `block_url`),
  do NOT set it -- bake the head into the base URL and leave `head` unset:
  ```yaml
  archive:
    url: http://127.0.0.1:8550               # your LOCAL archive; the re-derivation lands here
    token_file: /etc/bloar/token
    head: all
  upstream:
    url: https://audited.example.org/all     # the archive under audit -- head in the PATH
    block_url: http://independent-beacon:5052 # independent existence authority (unpruned blocks)
    # head UNSET -> anchored mode; the URL above is the untrusted PRIMARY byte source,
    # and there is no fallback, so a blob it cannot serve fails the batch.
  ```
  A batch failure names the slot the archive could not satisfy. Query that slot on
  the audited archive itself (`GET /all/eth/v1/beacon/blobs/{slot}`) to classify it:
  a covered-empty answer where the block feed says the slot carries blobs is a
  **completeness omission**; a 503 is a transient **byte-availability** gap.
- **(i) Completeness only, without trusting the audited archive's bytes.** If you
  have an INDEPENDENT full-history byte source (another archive you trust, or a
  provider), re-derive the head from THAT and never ask the audited archive at all.
  Start from [`backfill-all.yaml`](../deploy/examples/backfill-all.yaml) or
  [`beacon-all.yaml`](../deploy/examples/beacon-all.yaml) with `block_url` at the
  independent beacon node, and compare at the captured boundary `S` per the procedure
  above -- do NOT compare live current roots. This never exercises the audited
  archive's byte-serving at all; that is the trade against (ii), which does (a
  dropped blob fails a batch there rather than showing up as a boundary mismatch
  here).
- **A filtered (chain) head:** either shape, re-derived under the SAME captured
  manifest schedule (step 1), plus an independent L1 re-scan under that **published
  schedule** (§7.5, spec 10.5), then the same boundary comparison at `S`. Completeness
  is then proved **relative to the advertised schedule** -- a posting the schedule
  itself excludes is out of scope by construction (that boundary is inherent to a
  filtered head, spec 11.5).

### 7.5 Publishing a manifest upgrade

A chain head's filter is a published, append-only schedule of sources -- its
**manifest chain** (spec 10.5). Advancing it is an operator action, and the
supported way to do it is the indexer's **preflight**: `bloar-index
publish-manifest -config <path>`, run against the config carrying the new
`chain.sources`. The preflight is what makes the change safe -- only the indexer
sees L1, so only it can check the new schedule is a legal append-only successor
(spec 10.5). It:

- reads the head's current tip and position, loads and decodes the predecessor
  manifest, and refuses a change to any rule the head's position has already
  covered (naming the recovery order below);
- binds the publish to the head root it validated against, so a refs commit
  landing mid-publish is a `409` it re-runs against the advanced head, rather than
  publishing against a position that has moved; and
- on success prints the new tip CID.

This is authenticated (spec 7.3), and it is the one authenticated command an
operator runs *by hand* rather than as a service. The installed config carries the
systemd-credential form of `archive.token_file` (`${CREDENTIALS_DIRECTORY}/token`,
§3.1), which resolves only inside a unit -- so run by hand, deliver the token one
of two ways:

```bash
# publish-manifest reads the schedule from chain.sources and preflights it against
# L1 before posting, then prints the new tip.

# (a) Point -token-file at the source directly. It overrides the config's
#     credential form with a plain path; run as root, since the source is 0600
#     root:root (§3.1). Simplest. The config path is the instance file the unit
#     runs (bloar-index@chain-arbitrum-one -> /etc/bloar/index/chain-arbitrum-one.yaml).
sudo bloar-index publish-manifest \
  -config /etc/bloar/index/chain-arbitrum-one.yaml \
  -token-file /etc/bloar/token
# -> bafy...tip

# (b) Or have systemd deliver the credential for this one command, leaving the
#     installed config untouched -- the same handoff the service uses:
sudo systemd-run --pipe --quiet \
  --property=LoadCredential=token:/etc/bloar/token \
  -- bloar-index publish-manifest -config /etc/bloar/index/chain-arbitrum-one.yaml

# Then restart the indexer with the same config: startup requires the running
# schedule to EQUAL the published tip (spec 10.5), which it now does.
systemctl restart 'bloar-index@chain-arbitrum-one'
```

The bare `POST /bloar/v1/heads/{head}/manifest` endpoint (spec 7.2) still exists,
but it is **not** a semantically-checked path: the server compares CIDs and
confirms the head name, and has no L1 view to judge whether the schedule is a legal
successor, so a `200` from it is no promise the indexer will accept the schedule.
It now also requires an `expected_head_root` matching the head's current root,
which the preflight captures for you; a hand-rolled POST that omits it, or carries
a stale one, is refused. Reach for it only to inspect the wire shape, never as the
way to change a live head's filter.

A `409` from the preflight means the tip or the head root moved under you -- the
preflight re-runs itself against the current head, so a persistent `409` means
something else is publishing or the indexer cannot keep up with refs; investigate
rather than retry blindly.

#### Planned source transition on one or more writers

Use this procedure whenever the rule that identifies one chain's blob postings
changes: a `blob-txs` sender rotation, a move between inbox contracts, or a move
between `inbox-events` and `blob-txs`. The activation coordinate is an **L1 block
number** `B`, not a beacon slot. Source ranges are expressed in L1 blocks, and
the onchain transaction or contract state which authorizes the new poster also
takes effect in L1-block order.

Manifest `v` is the schema version, not the schedule revision. An ordinary
transition keeps `v: 1` and appends a new Manifest whose `prev` is the current
tip. Express a strict handoff as close-and-add:

```yaml
chain:
  sources:
    - type: blob-txs
      address: "0xOLD_OR_SHARED_RECIPIENT"
      senders: ["0xOLD_SENDER"]
      from_block: <original start>
      until_block: <B - 1>
    - type: blob-txs
      address: "0xNEW_OR_SHARED_RECIPIENT"
      senders: ["0xNEW_SENDER"]
      from_block: <B>
```

The same shape applies to an inbox move: close the old `inbox-events` source at
`B-1` and add the new contract/topic from `B`. If both mechanisms are
authoritatively valid during a migration, overlapping ranges are allowed and
deduplicated; do not invent a gap to avoid overlap. Do not leave a retired
sender open-ended merely to make rollout easier -- that would keep a retired or
compromised posting key authorized forever.

For a logical archive with multiple writers:

1. Obtain `B`, every address/topic/sender, and their authorization from the
   authoritative onchain change or chain-operator notice. Record the evidence;
   do not infer a sender from one observed transaction.
2. Choose or learn `B` while it is still strictly ahead of every writer's
   covered L1 position. Stop every affected chain indexer before the first one
   can cover `B`. Stopping is the simple, deterministic gate; finality lag is
   useful time, not the synchronization mechanism.
3. Capture each writer's current manifest tip and head document. Every writer
   must begin from the same tip. If one does not, remove it from advancement and
   repair its exact manifest ancestry before proceeding.
4. Put the **same ordered `chain.sources` schedule** in every indexer's config.
   Writer URLs and token paths may differ; head name and source schedule may not.
5. Run `bloar-index publish-manifest` once against each writer, with that
   writer's config and credential. Each preflight binds to that writer's own
   current head root, so writers need not have equal coverage, but every command
   must print the **same new manifest CID**. `expected_head_root` is request
   concurrency state and is not part of the Manifest CID.
6. Only after every admitted writer has the identical tip, start the indexers
   under the matching configs. Verify the published documents carry that tip,
   coverage advances on every writer, and roots are equal at equal coverage (or
   exact prefix projections at unequal coverage).

Do not roll a manifest tip backward if the planned transition is cancelled
before `B`. Publish another descendant whose full schedule removes or supersedes
the still-future rule; the preflight permits that only while the rule remains
ahead of coverage. The manifest chain records both operator decisions.

**Changing a rule the head has already covered** (a bad allowlist that admitted a
blob, say) is refused by the preflight's append-only check, and the only order that
passes is mechanical: **truncate** the head to before the affected range (§5.4,
7.2), **publish** the corrected manifest (now legal, since the position sits
behind the changed rule), then **resync**. Manifest-first is rejected; truncate
-first is what makes it pass. A `manifest_ancestry` refusal on your followers
(§6.1) means a manifest was published that discards the chain they hold -- most
often a recovery attempted out of order, or a chain minted afresh. §7.6 walks the
most common case of this end to end.

#### Missed or incorrect source transition

If a writer covers `B` under the old rule, the condition is recoverable. Treat
it as a controlled application-level rewind and re-derivation, not an L1 reorg
and not a follower reset:

1. Stop every affected chain indexer. If writers now make incompatible claims,
   remove the incorrect source from follower advancement until it is repaired;
   never majority-vote through an equal-coverage root mismatch.
2. Establish the authoritative `B` and corrected full schedule. Map `B` to its
   beacon slot using the configured genesis time and seconds per slot, and choose
   a truncation slot `S` below it so the next scan necessarily includes `B`.
3. Truncate each writer's affected head to `S`.
4. With coverage now behind the changed rule, run the normal manifest preflight
   against every writer and require the same descendant CID everywhere.
5. Restart the indexers under the matching schedule and let them re-derive past
   their former frontier. Require equal roots at equal coverage before returning
   a repaired writer to the active source set.

Followers deliberately reject the truncated documents and every intermediate
document below their durable `synced_to` floor. They keep serving last-good and
adopt automatically after corrected coverage climbs past that floor. The
corrected manifest is accepted because its `prev` lineage still contains the
tip they hold. Do not restart followers, clear their floors, or repoint clients
during this interval.

If `S` is behind `server.immutable_horizon_slots`, purge any CDN or client cache
which could hold responses for the corrected range; those responses may have
been served with a one-year immutable lifetime. A rewind inside the horizon
needs no purge. The concrete command sequence and observability checks are in
the missed-source example below.

### 7.6 Missed-source recovery

The situation: a head was synced under a filter that was missing a source. A
chain moved to posting blobs from a plain EOA -- a `blob-txs` source (spec 10.4)
-- at some L1 block `B`, but the head's `chain.sources` only ever had the
`inbox-events` source. From `B` on, the head's history is short exactly the blobs
that source would have selected: they were never referenced, so the head answers
`404` for them at slots it otherwise covers.

This is not an append. The missed range sits **behind** the head's position, so
correcting it rewrites covered history, and the only order that passes is
truncate-first (spec 5.4, 10.5): move the position back below `B`, publish the
corrected schedule (now legal), then let the indexer re-derive. Publishing it
**without** truncating first does not work, and the preflight says so: `bloar-index
publish-manifest` maps the head's `synced_to` to an L1 block, sees the new source
activates behind it, and **refuses** with an error naming this order (spec 10.5).
And a running indexer will not paper over it either -- its startup
requires the config to equal the published tip, so a corrected `chain.sources`
cannot run until its manifest is legally published. Those refusals are the
guardrail, not a fault -- they are the validation, and there is no separate dry-run
to perform.

1. **Stop the indexer for the head.** Nothing should be advancing `synced_to`
   while you move it. The chain head's instance is `chain-<head>` (§3.1, the unit
   header), so for `arbitrum-one` that is `bloar-index@chain-arbitrum-one`.

   ```bash
   systemctl stop 'bloar-index@chain-arbitrum-one'
   ```

2. **Compute the boundary slot.** Truncate to a slot **at or below** the one `B`
   lands in. The indexer resumes at the L1 block just past `synced_to` (spec
   10.2), so a `synced_to` below `B`'s slot re-scans `B` on the next pass. `B`'s
   slot is the indexer's own arithmetic, `slot = (timestamp(B) - genesis_time) /
   seconds_per_slot`; take one below it to be safe against two blocks sharing a
   slot. Call the result `S`.

3. **Truncate the head to `S`** (spec 5.4, 7.2). `confirm` is required and must be
   the head's own name; this discards coverage.

   ```bash
   # The token source is 0600 root:root (§3.1), so read it as root.
   curl -sS -X POST "$ARCHIVE/bloar/v1/heads/arbitrum-one/truncate" \
     -H "Authorization: Bearer $(sudo cat /etc/bloar/token)" \
     -d '{"confirm":"arbitrum-one","slot":<S>}'
   # -> {"synced_to":<S>,"root":"bafy...truncated"}
   ```

   `synced_to` drops to `S` and the root changes. If `S` is a window-final slot
   the archive seals the window as it truncates (spec 5.4); nothing to do about
   that, but it is why the root moves even when `S` lands on a boundary.

   On a writer configured with `pin.mode: window`, a rewind moves the trailing
   retention window backwards. Before publishing, bloard validating-reads the
   rebuilt Segment and any older sealed Segment closures which become newly
   retained. A deep rewind can therefore cost I/O proportional to the newly
   included range and fails closed if those formerly out-of-window blobs were
   already reclaimed or are corrupt. Restore/re-ingest the missing blocks, or
   choose a boundary whose new retention window is locally complete, then retry.
   `full` already retained the closures; `none` does not claim to retain them.

4. **Add the source to `chain.sources` and publish the corrected manifest** (§7.5)
   with the preflight. The new schedule is a plain **add** of the `blob-txs` source
   from `B` alongside the unchanged `inbox-events` source -- not a close-and-add,
   because the inbox source was never wrong and keeps governing its own blobs; what
   was missing is a whole second posting mechanism. `publish-manifest` reads the
   schedule from `chain.sources`, so edit the config first, then run it.

   ```bash
   # Edit /etc/bloar/index/chain-arbitrum-one.yaml: add the blob-txs source from <B>.
   # publish-manifest is authenticated and run by hand, so deliver the token the
   # same way §7.5 does -- the installed config carries the credential form.
   sudo bloar-index publish-manifest \
     -config /etc/bloar/index/chain-arbitrum-one.yaml \
     -token-file /etc/bloar/token
   # -> bafy...corrected
   ```

   The preflight now **passes** where it refused before truncating: the position
   sits below `B`, so the added source is a legal append ahead of it (spec 10.5).
   It is the preflight, not the server, that enforces this -- the server has no L1
   view -- and truncate is what moves the position to make the enforcement pass.

5. **Restart the indexer.** With the corrected manifest published and
   `chain.sources` already matching it, startup's equality check passes and the
   indexer re-derives the truncated range, this time selecting both sources' blobs.

   ```bash
   systemctl start 'bloar-index@chain-arbitrum-one'
   ```

**What to watch.** `synced_to` for the head dips to `S` at the truncate and climbs
back as the indexer re-derives; that climb is the recovery. On the re-covered
range the previously-missing blobs now serve, and the head's root ends up
different from before -- the union added versioned hashes.

Your **followers refuse every document through the dip, and that is correct**.
A follower holds a `synced_to` floor at the coverage it last adopted (spec 11.3),
and the truncated document -- coverage below that floor -- is declined as a
regression, exactly as a withheld or rolled-back document would be. The counter
`bloar_follow_refusals_total{reason="synced_to_floor"}` (§6.1) ticks up for the
duration; that counter moving here is the system working, not breaking. **Do not
"fix" a follower during the dip** -- do not restart it, re-point it, or clear its
floor. Each one freezes at its floor, goes on serving its last good state, and
adopts the recovered root automatically the moment coverage climbs back past the
floor. The
new manifest tip is accepted at that adoption because its `prev`-lineage walks
back through the tip the follower already holds (a `manifest_ancestry` check, spec
11.3): a legal extension, not a rewritten history. Nothing operator-side is needed
on the followers.

**The deep-truncate caveat** (spec 5.4). If `S` falls below the immutable horizon
(`server.immutable_horizon_slots`, default one day behind `synced_to`; spec 7.1),
the slots you truncated past were served
`Cache-Control: public, max-age=31536000, immutable`, and a CDN or client cache
in front of the archive may hold those answers for a year. Truncate changed what
those slots should return, so the cached responses for the affected range **must
be purged** from the CDN -- the archive cannot invalidate a cache it does not
run. A truncate that stays within
the horizon (the common case, catching a recent misconfiguration) needs no purge:
those slots were served `max-age=60`.

### 7.7 Backfilling past the local beacon's blob horizon

The situation: the ALL head needs history older than the local beacon node still
holds blobs for. A beacon node keeps blobs for only a retention window (~18 days
on mainnet) and then prunes them, and past that window it does not even answer
consistently: it may `404` a pruned slot, or answer `200` with an empty data array
(prysm does the latter) -- the very same answers it gives a slot that never carried
blobs. What the node never prunes is its **blocks**, and a block is what proves
whether a slot carried blobs at all.

Post-Fusaka, do not infer complete-blob service from the words "full node" or
"archive node." PeerDAS deliberately lets an ordinary consensus node validate
availability while custodying only a subset of the 128 data columns; reconstructing
a complete blob requires at least half of the columns. Depending on the client,
complete serving is an explicit semi-supernode or supernode role. Long-term
retention is a second, independent property.

Retention flags are not retroactive acquisition. A node configured today to stop
pruning may retain everything it receives from today onward while still lacking
older blobs that were never fetched or were already pruned. Some clients require
a fresh checkpoint sync with a separate blob-backfill mode to acquire older
columns at all. Before treating an endpoint as a writer's complete source, test
real non-empty slots at all three boundaries:

1. the current custody window;
2. the oldest slot the node claims to retain; and
3. the intended archive origin.

For each probe, compare the returned versioned hashes with the commitments in the
corresponding beacon block. A `200` alone is not evidence: a pruned Prysm endpoint
has returned `200 {"data":[]}` for a block that demonstrably carried blobs.
Record the measured serving frontier, client mode, database creation/sync
procedure, retention setting, and whether historical acquisition was actually
performed. Re-run the boundary probes after client upgrades or source changes.

The protocol distinction is specified by
[EIP-7594](https://eips.ethereum.org/EIPS/eip-7594); Lighthouse's
[data-column guide](https://lighthouse-book.sigmaprime.io/advanced_blobs.html)
is a concrete example of the separate serving, retention, and fresh-backfill
controls. Other clients may expose different flags, but the three properties
remain separate and must be measured rather than inferred.

The beacon indexer runs in **anchored** mode (spec 10.1; the `index/beacon`
package comment is the full story): a **trusted block feed** -- the node's block
API, never pruned -- decides which slots carried blobs and fixes each slot's
versioned hashes from the block's own KZG commitments. Every blob endpoint is an
ordered, **untrusted** byte source whose bytes are accepted only when they commit
to those block-derived hashes. Absence is never taken from a blob source.

This correctness boundary does not make missing historical bytes appear.
Before an empty-store rebuild, prove that the selected source actually serves
complete blobs back to the configured origin. If it does not, restore a reviewed
Bloar backup or replicate from an authenticated Bloar archive whose published
coverage includes that origin. Do not substitute an unowned public service as an
implicit emergency source. Start from
[`backfill-all.yaml`](../deploy/examples/backfill-all.yaml) only when the beacon
source has passed those boundary probes; use mirror mode for authenticated Bloar
replication.

Run exactly one indexer against a head. A head has a single `synced_to`
watermark, so a second indexer never fills a different range -- it races the
same one, which is wasted work. A non-anchored indexer racing it can also record
incorrect coverage decisions that later replay cannot repair.

### 7.8 Corrupt local blocks: detection and repair

Every block read out of the local store is validated against the CID it was asked
for: the bytes are re-hashed and compared, and a mismatch is refused rather than
served. flatfs keys blocks by multihash, so a byte flipped on disk
-- bit rot, a truncated write, a block altered in place -- leaves the wrong bytes
under the right key, and without this check the read path would hand them back
under a CID they no longer match. The public read API answers such a read `500`
(not the `404` a missing blob gets, nor the `503` a slow fetch gets) and counts it
in **`bloar_store_corrupt_reads_total{head}`**. Any nonzero value there means this
node is holding a corrupt block a live root references: it is a page to act on, not
a transient.

**Finding them: `bloard fsck`.** Run with the daemon **stopped** -- it takes the
store's lock, the same as `rebuild` (§7.3):

```
systemctl stop bloard
sudo -u bloar bloard fsck -config /etc/bloar/bloard.yaml            # report only
```

`fsck` walks the pinned closure of every head in the config (plus the reserved
staging head; `-head H` scopes it to one), validates every block it reaches, and
prints a per-CID listing of what is `corrupt` and what is `missing` (a pinned block
absent locally -- a dangling pin, a different fault). It exits nonzero if it found
any corruption, so a cron or a one-off can gate on it. It **deletes nothing**
without `-repair`.

**Repairing them** is per class of block, because what can regenerate a block
depends on what it is:

- **A follower, any block.** Delete it. The fetching blockstore refetches it from a
  peer on the next read (spec 11.4) -- a genuine self-heal, because a follower's
  blocks are replicas of a writer's and the writer still has them. This is the only
  case that heals by itself: `bloard fsck -repair` deletes the corrupt block, and
  the next read (or the next pin reconciliation pass, which walks the DAG and
  fetches every miss) brings it back. Do **not** run `-repair` against a follower
  while the daemon is up; stop it, repair, restart, and let it refetch.

- **A writer, a raw blob leaf.** The writer is the source, so nothing on the network
  has the blob to give back -- it must come from outside bloar. Delete the corrupt
  block and refill it with the correct bytes:

  ```
  bloard fsck -config /etc/bloar/bloard.yaml -repair     # deletes the corrupt block
  # obtain the blob's 131072 bytes out of band: a beacon node, another archive,
  # or a re-derivation, keyed by the CID fsck listed.
  bloard put-block -config /etc/bloar/bloard.yaml -cid <cid> ./blob.bin
  ```

  `put-block` re-hashes the file and refuses to write unless its bytes reproduce
  **exactly** the stated CID, so it can only ever store a block that is what it
  claims to be. `-repair` deletes first because flatfs skips a Put whose key already
  exists: without the delete, `put-block` would refuse to no-op over the corrupt
  block and tell you so.

- **A writer, a DAG-CBOR index node.** These are not automatically recoverable and
  bloar does not pretend to recover them: an index node is derived state, and the
  supported answer is to **restore `blocks/` from a backup** (§4) taken before the
  corruption, or to **fetch the node by its CID from a peer** that holds the same
  head (another archive, or a follower) and `put-block` it. If neither is available,
  the head must be rebuilt from its byte sources (§7.4). This is an operator
  procedure, deliberately not automation: silently reconstructing a spine node is
  exactly the kind of thing that should never happen without a human deciding it.

**Minimum requirements (the cost of always-on validation).** Validation computes
`sha2-256` over every block on every read. Bloar caches decoded index nodes but
never raw blocks, so every blob read pays a full hash of its approximately
131 KiB payload. Size this work from measurements on the target CPU and
filesystem; results from a development machine are not a deployment contract.

The repository keeps paired on-disk OFF/ON benchmarks over one shared fixture.
Run several counts against a scratch directory on the same filesystem that will
hold the archive:

```sh
set -eu
bench_parent=/absolute/path/on/target/filesystem
test -d "$bench_parent"
bench_root="$(mktemp -d -p "$bench_parent" bloar-validation-bench.XXXXXXXX)"
cleanup_bench() {
  case "$bench_root" in
    "$bench_parent"/bloar-validation-bench.*) rm -rf -- "$bench_root" ;;
    *) echo "refusing unsafe benchmark cleanup: $bench_root" >&2; return 1 ;;
  esac
}
trap cleanup_bench EXIT HUP INT TERM

BLOAR_BENCH_STORE_DIR="$bench_root" \
  go test ./server -run '^$' \
    -bench '^(BenchmarkOnDiskRawBlobGet|BenchmarkOnDiskIndexNodeGet|BenchmarkOnDiskBlobReadJSON|BenchmarkOnDiskBlobReadOctet|BenchmarkOnDiskApplyRefs64)$' \
    -benchmem -count=5
```

Compare each `/off` and `/on` pair. `BenchmarkOnDiskGCMark` is an observational
integration harness over the combined validation path; do not use it to size
production's separately scheduled GC and scrub. Retain the raw output with that
deployment's evidence, together with the CPU, filesystem, kernel, Go version,
source commit, and whether the host was otherwise idle. The public read endpoint
pays one hash per blob served, and `apply_refs` pays one hash per blob committed,
so capacity must cover the expected origin traffic plus ingest rather than a
copied throughput figure.

GC and scrub have different cost models. GC validates DAG-CBOR blocks because
their bytes supply outgoing links; raw targets need only an existence check
before their multihash is marked. GC mark cost therefore scales with structural
DAG bytes, block enumeration, and set overhead rather than every retained blob
byte.

The separately scheduled scrub owns the full integrity pass. CID validation
hashes the whole block, so estimate its duration as
**`scrub_validated_bytes / T + scrub_scanned_blocks × O`**, where `T` is the
target host's measured hash throughput and `O` its measured per-read framing
overhead. Flatfs enumeration visits each stored multihash once regardless of
how many heads pin it, so scrub has no per-head pin amplification; it does
include unreachable garbage until GC removes it.

GC still checks raw targets for existence and validates DAG-CBOR direct pins;
missing written data fails closed, and eligible follower misses can still heal.
Scrub also checks unreachable stored objects but neither deletes nor refetches
them. Require both jobs to finish within their configured cadences. If either
approaches its interval, widen the interval, schedule scrub for a quieter I/O
window, provision more CPU or faster storage, or use a windowed retention
policy. Hosts without hardware hash acceleration need especially careful
measurement before committing to a serving rate.

---

### 7.9 Anchored indexer near genesis

An anchored beacon indexer proves every 404 by parent-root continuity: a slot the
block feed 404s is a real miss only if a later present slot chains cleanly over it
(spec 10.1). On the first batch of a run it seeds that anchor by walking headers
back from the resume point to the most recent present slot. On mainnet this is
never an issue -- the origin sits millions of slots above genesis and the walk
finds a present header within a slot or three. On a **young or dev network** whose
`origin_slot` is within ~1024 slots of slot 0, a feed that is still backfilling
historical blocks can 404 that whole range, and the walk then reaches slot 0 with
no anchor.

When that happens the indexer **waits, indefinitely and retryably** -- it never
guesses an anchor, because committing coverage over an unproven leading 404 is
permanent under idempotent replay (spec 5.1). You will see it stop advancing with a
log line `continuity anchor cannot be seeded yet`. Two ways forward:

- **A later `origin_slot`.** If the head has no coverage yet, raise its origin past
  the unbackfilled region to a slot the feed does have. Simplest when nothing is
  committed.
- **`upstream.continuity_checkpoint`.** A trusted `{slot, root}` the walk stops at
  and anchors to. Use it when the origin must stay put. Rules:
  - `slot` must be **strictly before the current resume start** -- the first slot this
    run covers, which is `synced_to + 1`, or `origin_slot` only when the archive has
    no coverage yet -- so the checkpoint can never itself advance coverage. A slot at
    or above the resume start is a fatal error, refused **before the indexer next
    covers a slot**: the check runs as part of seeding the anchor, and a caught-up pass
    (nothing new finalized to cover) returns before it, so a bad checkpoint surfaces
    the moment there is a batch to plan, not necessarily at process start. (A checkpoint
    set for a fresh archive can also become redundant once coverage advances past it;
    the run just stops walking back far enough to reach it.)
  - `root` is a **0x-prefixed 32-byte** block root you have verified out of band
    (from a trusted archive, a block explorer, another synced node). It is trusted:
    the walk anchors to it whether the feed 404s that slot or serves it.
  - The checkpoint is only consulted **when the seed walk actually reaches its slot**
    (the walk stops at the first present header, so a present slot above the checkpoint
    anchors instead and the checkpoint is never read this run). When the walk does
    reach it and the feed's header there is present but its root **disagrees** with the
    configured one, the indexer refuses to run -- the feed and your checkpoint cannot
    both be right about the anchor everything chains to, so nothing advances until you
    reconcile them. Fix the config or the feed; do not delete the check.

A checkpoint is a per-run trust assertion, not a persisted anchor: it seeds the
first batch, after which continuity carries itself from present slot to present
slot. Once the feed has backfilled past the origin you can remove it. See the
commented block in [`deploy/examples/beacon-all.yaml`](../deploy/examples/beacon-all.yaml).

---

## 8. Notes on the container image

[`deploy/Dockerfile`](../deploy/Dockerfile) builds the production binaries
`CGO_ENABLED=0` into distroless/static. The default `bloar` image contains
`bloard`, the indexer, standalone Kubo replica, and swarm inspector. The
separate `edge` target contains only `bloar-edge`; releases publish it as the
independently digest-pinned `ghcr.io/blobarchive/bloar-edge` artifact. Build the
default through `make docker IMAGE=<tag>` and the edge through
`make docker-edge IMAGE=<tag>`. Both production builds require a clean exact Git
commit, refuse dirty or unstamped binaries, and write the same revision plus
source and creation time into OCI image labels. The ordinary
repository context is deny-all: the Makefile generates a context containing
only tracked Go source plus `go.mod`/`go.sum`, and supplies a minimal
one-commit, tree-only Git metadata snapshot as a BuildKit secret. The
Dockerfile extracts `.git` only into an ephemeral tmpfs mount. The snapshot has
no remotes, credentials, refs, reflogs, hooks, blobs, or prior history.
Repository metadata, checkout credentials, local secrets, evidence, docs, and
operator state therefore cannot enter the context or a committed layer.
`-buildvcs=true` is necessary but not sufficient for the provenance guarantee:
Go may still emit an unstamped binary
when repository metadata is unavailable. The Dockerfile's explicit
`go version -m` comparison against the requested revision is the load-bearing
check; do not remove it as redundant with the flag. The same loop requires the
permanent public Go module path, so an image cut from a pre-rename tree cannot
claim the public repository only through its OCI label. Public release policy
and digest/attestation verification are in [releases.md](releases.md).

Both Make targets disable Docker layer-cache reuse for the
provenance-verification step. BuildKit does not include secret contents or even
required-secret presence in a layer cache key, so a cached layer could otherwise
skip reevaluating the VCS snapshot mount. Go module and compiler cache mounts
remain enabled.

`CGO_ENABLED=0` is a real choice about KZG, not just about linking. go-ethereum's
`crypto/kzg4844` uses the cgo `c-kzg-4844` library when cgo is on and the pure-Go
`gokzg4844` when it is not, so this image verifies blobs in Go. That is slower per
blob -- it is the hot path of ingest, and `bloar_ingest_kzg_verify_duration_seconds`
is where you would see it -- and it buys a static binary with no C toolchain in
the runtime image and no glibc version to match. Both are correct; the conformance
suite passes either way. Build with `CGO_ENABLED=1` and a libc-bearing base if
ingest throughput turns out to matter more than the deployment story.

The image runs as `nonroot` (uid 65532). The store volume must be writable by it.
