# bloar: Data Structures, Illustrated

Companion to [spec.md](spec.md) (sections 3-5). All numbers below are real:
the ALL head with `seg_bits 9`, `fanout_bits 8`, `origin_slot 8626176`
(so `dir_base = 8626176 >> 9 = 16848`), and the arbitrum-one head with
`seg_bits 13` (`dir_base = 1053`).

Legend: `o-->` is an IPLD link (a CID), `[blob]` is a raw 128 KiB block,
`*` marks a newly written block.

## 1. One head, top to bottom

```
    the only mutable thing              everything below this line is
    (signed JSON over HTTPS)            immutable and content-addressed:
                                        one CID names the entire state
    GET /bloar/v1/heads                 of a head at one instant
    { "all": bafy...H1, ... }
              |
              v
    +--------------------------------------+
    | Head "all"                (dag-cbor) |
    |--------------------------------------|
    | origin_slot 8626176    seg_bits    9 |
    | synced_to  12346123    fanout_bits 8 |
    | dir_depth 2                          |
    | open o-----------.                   |
    | dir  o-----------|--------.          |
    +------------------|--------|----------+
                       |        |
       .---------------'        v
       |            +------------------------------+
       |            | DirNode (root)               |
       |            | kids: [0] [1] ... [28] ...   |   up to 256
       |            +--------o---o--------o--------+   links per page
       |                     |   |        |
       |                     v   v        v
       |                  page  page   +--------------------------+
       |                   .     .     | DirNode (page 28)        |
       |                   .     .     | kids: ... [95] [96]      |
       |                               +------------o----o--------+
       |                                            |    |
       |                        .-------------------'    |
       v                        v                        v
    +----------------+   +--------------+        +--------------+
    | Segment (open) |   | Segment      |        | Segment      |
    | ord 24113      |   | ord 24111    |        | ord 24112    |
    | grows in place |   | sealed:      |        | sealed:      |
    |                |   | write-once   |        | write-once   |
    +---o------o-----+   +--o------o----+        +---o------o---+
        |      |            |      |                 |      |
        v      v            v      v                 v      v
     [blob] [blob]       [blob] [blob]            [blob] [blob]

     [blob] = raw IPFS block, exactly 131,072 bytes, one block per blob,
              shared by every head that references it
```

## 2. A blob's two names

```
                  +----------------------------------+
                  |     blob bytes  (131,072 B)      |
                  +----------------------------------+
                     |                          |
     sha2-256 of     |                          |   KZG commitment, then
     the raw bytes   |                          |   0x01 || sha256(cmt)[1:]
                     v                          v
      CID  bafkrei...                versioned hash  0x01c2...
      -----------------              -----------------------------
      "where it lives":              "what the chain calls it":
      names the IPFS block           appears in blob txs and in the
                                     sequencer inbox message

      Neither is derivable from the other without the blob itself,
      which is why every segment entry stores the pair  [vh, &blob]
      and the daemon keeps a local vh -> CID catalog for ingest.
```

## 3. Segment anatomy

```
   Segment, ord 24112        window = [ord << 9, (ord+1) << 9)
                                    = slots 12,345,344 .. 12,345,855
  +---------------------------------------------------------------+
  | v: 1                                                          |
  | slot0: 12345344                                               |
  | rows:                                                         |
  |   [ 12345347, [ [0x01a4.., &blob], [0x01f0.., &blob] ] ]      |
  |   [ 12345352, [ [0x0119.., &blob] ] ]                         |
  |   [ 12345391, [ [0x01c2.., &blob], [0x017e.., &blob],         |
  |                 [0x01d8.., &blob] ] ]                         |
  |     ...                                                       |
  |   [ 12345850, [ [0x0163.., &blob] ] ]                         |
  +---------------------------------------------------------------+

   - rows hold only blob-carrying slots, ascending, no duplicates
   - a covered slot absent from rows provably has no blobs
     (coverage advanced through it; there is no "gap" state)
   - entry order within a row = blob order in the beacon block
   - a window with zero rows seals to nothing: a null directory entry
```

## 4. Lookup is arithmetic, not search

```
   find slot 12,345,678 with vh 0x01c2..

     ord  = 12345678 >> 9      = 24112
     idx  = 24112 - 16848      =  7264
     path = 7264 in base 256   = [28, 96]      (dir_depth = 2 digits)

     Head.dir
        |
        v
     DirNode(root) --kids[28]--> DirNode(page) --kids[96]--> Segment
                                                                |
                                             binary-search rows for slot
                                             12345678, match vh in row
                                                                |
                                                                v
                                                        &blob -> [blob]

   No stored keys anywhere on the route: window boundaries are fixed by
   seg_bits, so the path digits ARE the address. A null at any hop means
   "no blobs anywhere in that subtree's slot range".
```

## 5. Sealing a window: copy-on-write spine rewrite

When `synced_to` crosses a window boundary, the open segment's CID is
appended to the directory. Only the spine to that slot is rewritten.

```
   sealing window idx 7264 (path [28, 96]); * = newly written block

        before                              after

   Head --- dir --> R                  Head* --- dir --> R*
     |              |                    |               |
     |     kids[28] |                    |      kids[28] |
     |              v                    |               v
     |              P                    |               P*
     |         (kids 0..95)              |          kids[96] o
     |                                   |                  |
     |                                   |                  v
     '--- open --> S                     |                  S   <- same
              (ord 24112, window         |                       bytes,
               now fully covered)        '--- open --> E*        same CID,
                                                (ord 24113,      now sealed
                                                 empty)

   written:  Head*, R*, P*, E*  -- three tiny pages + an empty segment
   reused:   S itself and every other page and segment in the head,
             byte-identical, same CIDs (structural sharing)
   orphaned: old Head, R, P -- swept at the next GC unless pinned
```

## 6. Many heads, one copy of the data

```
    Head "all"  (seg_bits 9)             Head "arbitrum-one"  (seg_bits 13)
        |                                     |
      (dir)                                 (dir)     wider windows because
        |                                     |       rows are sparse: only
        v                                     v       slots the SequencerInbox
    Segment ord 24112                    Segment ord 1507        references
    rows:                                rows:
     [ 12345347, [ [vh1, &b1],            [ 12345347, [ [vh2, &b2] ] ]
                   [vh2, &b2],              ...
                   [vh3, &b3] ] ]
       ...      |          \                       |
                |           \                      |
                v            \                     v
              [b1]  [b3]      '----> [b2] <-------'
                                      ^
                                      one 128 KiB raw block, two owners

   Same bytes -> same CID -> same block: cross-head sharing is not a
   feature that can have bugs, it is what content addressing means.
   Marginal cost of another head: ~80 bytes of index per referenced
   blob, < 0.1% of the blob itself.
```

## 7. Retention = pins over segments

```
   all:           { mode: full }     one recursive pin on the Head root
   arbitrum-one:  { mode: window, duration: 720h }

   arbitrum-one directory, time increasing to the right
   (720h = 216,000 slots = ~27 windows at seg_bits 13):

   idx:    0       1     ...    427    428   ...    453      (454)
        [seg]---[seg]--- ... -[seg]--[seg]-- ... -[seg]----(open)
                                       |                       |
                                       +----- 720h window -----+
                                       recursive pin on every
                                       segment in the window,
                                       and on the open segment
          direct pins: Head root + every DirNode page
          (index stays complete; only blob retention slides)

   GC is mark-and-sweep from the union of ALL pins across ALL heads:
   a blob outside arbitrum-one's window still survives if the ALL
   head's full pin (or any other head) reaches it. Dropping the last
   policy that reaches a blob is the only thing that ever deletes it.
```
