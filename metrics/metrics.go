// Package metrics is bloard's Prometheus instrumentation and its liveness and
// readiness probes (spec 7.4).
//
// # Nil is the disabled state
//
// Every method here is nil-safe, and *Metrics is threaded through the configs
// of the packages that have something to say (server, ingest, pinning, p2p) as
// an optional field. A daemon with `server.metrics_listen` unset builds no
// registry, passes nil everywhere, and the instrumentation costs a nil check at
// each seam. This is the whole reason the type is a struct of collectors rather
// than a set of package-level vars: package-level vars would register
// themselves in a global registry whether or not anyone asked, and two daemons
// in one test binary would panic on the duplicate registration.
//
// # Cardinality
//
// Every label in here is bounded by the config or a closed set. `head` is the
// heads the node serves -- a handful. A multi-writer `source` is admitted only
// from the exact locally configured head/source authorization relation.
// `purpose` is pinning's five head purposes plus staging. `status` is an HTTP
// status class (2xx/4xx/5xx), not a status code. `reason`, `outcome`, p2p
// `direction`/`transport`, and Bitswap `peer_class`, rendezvous
// operation/outcome, pointer kind/retry reason, and IPNS publication stage are
// closed sets of constants defined in this package.
//
// Nothing is labelled by slot, by CID, by versioned hash, or by peer. Those are
// the four unbounded dimensions bloar has, and a single one of them in a label
// would grow the registry without limit for the life of the process: an archive
// serves millions of slots, and Prometheus keeps a time series per label
// combination it has ever seen. Where per-slot detail is wanted, the logs have
// it.
package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// namespace prefixes every metric name.
const namespace = "bloar"

// Segment sizing states and index-node kinds are closed metric dimensions.
// Callers must use these values; CIDs, slots and window ordinals never become
// labels.
const (
	IndexSegmentOpen   = "open"
	IndexSegmentSealed = "sealed"

	IndexNodeHead    = "head"
	IndexNodeDir     = "dir"
	IndexNodeSegment = "segment"

	// These are the initial operational alert thresholds. The hard ceiling is
	// owned by archive.MaxIndexNodeBytes and is not duplicated here.
	IndexSegmentWarningBytes  = 1 << 20
	IndexSegmentCriticalBytes = 3 << 19
)

// Ingest rejection reasons (the `reason` label of bloar_ingest_rejects_total).
// A closed set: these are the only ways spec 7.2's blobs endpoint refuses a
// body, and the HTTP layer's 400s map onto them.
const (
	RejectFraming = "framing" // body is not a whole number of blobs, or too many
	RejectKZG     = "kzg"     // a blob is not canonical field elements
	RejectStore   = "store"   // the write itself failed
)

// Outcomes for the counters that have them.
const (
	OutcomeOK    = "ok"
	OutcomeError = "error"
)

// Follower sync outcomes distinguish useful completion from the two benign
// reasons a coalescing worker can finish without stamping a generation. They
// are a closed set: a root or revision never becomes a metric label.
const (
	FollowSyncCompleted  = "completed"
	FollowSyncNoop       = "noop"
	FollowSyncSuperseded = "superseded"
)

// Edge publication outcomes distinguish a configured transaction deadline and
// process/request cancellation from an ordinary DHT error. They are separate
// from the older binary outcome vocabulary so existing series do not silently
// gain labels their callers never emit.
const (
	EdgePublicationOutcomeTimeout  = "timeout"
	EdgePublicationOutcomeCanceled = "canceled"
)

// Edge publication operations and terminal stages are closed observability
// dimensions. Identifiers such as CIDs, revisions, sequences, and authority
// names belong in structured logs, never metric labels.
const (
	EdgePublicationOperationPublish = "publish"
	EdgePublicationOperationRestore = "restore"

	EdgePublicationStageAdmission       = "admission"
	EdgePublicationStageValidation      = "validation"
	EdgePublicationStageProvideDocument = IPNSStageProvideDocument
	EdgePublicationStagePersistState    = "persist_state"
	EdgePublicationStagePutRecord       = IPNSStagePutRecord
	EdgePublicationStageComplete        = "complete"
)

// Edge DHT query events are the bounded, identity-free phases exposed by
// go-libp2p's query-event stream. A value_rpc event occurs only during
// PutValue: it means closest-peer discovery completed and final record fan-out
// began, not that the remote peer stored the record.
const (
	EdgeDHTQueryEventSendingQuery = "sending_query"
	EdgeDHTQueryEventPeerResponse = "peer_response"
	EdgeDHTQueryEventQueryError   = "query_error"
	EdgeDHTQueryEventDialingPeer  = "dialing_peer"
	EdgeDHTQueryEventValueRPC     = "value_rpc"
)

// Edge DHT lookup termination reasons are the closed outcomes exposed by
// go-libp2p's lookup-state stream. They describe logical lookup termination;
// the routing call may continue waiting for in-flight or follow-up work.
const (
	EdgeDHTLookupTerminationStopped    = "stopped"
	EdgeDHTLookupTerminationCancelled  = "cancelled"
	EdgeDHTLookupTerminationStarvation = "starvation"
	EdgeDHTLookupTerminationCompleted  = "completed"
)

// IPNS publication stages. The publisher completes these in order: the exact
// document CID is provided before the IPNS record is put.
const (
	IPNSStageProvideDocument = "provide_document"
	IPNSStagePutRecord       = "put_record"
)

// Public-read admission outcomes are a closed label set. They mirror the
// server limiter's decisions without importing server here (server already
// imports metrics), and are the only values PublicReadAdmission records.
const (
	PublicReadAdmitted         = "admitted"
	PublicReadRejectedGlobal   = "rejected_global"
	PublicReadRejectedClient   = "rejected_client"
	PublicReadRejectedCanceled = "rejected_canceled"
)

// GC phases are a closed label set. Keeping them here prevents callers from
// accidentally putting an error string, CID, or other unbounded value into a
// Prometheus label.
const (
	GCPhasePrepare = "prepare"
	GCPhaseMark    = "mark"
	GCPhaseSweep   = "sweep"
	GCPhaseCensus  = "census"
)

// Anchored blob-source outcomes (the labels of bloar_index_source_fetch_total).
// `source` names which upstream in the beacon indexer's ordered list answered;
// `outcome` is what it did with an anchored slot's filtered request (spec 10.1).
// A closed set: anchored mode derives a slot's expected versioned hashes from
// the trusted block feed, then tries each source in order until one is anchored.
const (
	SourcePrimary  = "primary"
	SourceFallback = "fallback"

	// SourceAnchored is a source that returned exactly the block's blobs, each
	// committing to its expected vh: the slot is recorded and no later source is
	// tried.
	SourceAnchored = "anchored"
	// SourceMismatch is a 200 whose bytes do not commit to the expected vh: the
	// source served the wrong blob, and the next source is tried.
	SourceMismatch = "mismatch"
	// SourceAbsent is a 404, or a 200 with the wrong blob count: this source
	// cannot help, and the next source is tried.
	SourceAbsent = "absent"
	// SourceError is a terminal failure or a 503 from a source: it cannot help,
	// and the next source is tried. Unlike mirror mode, an anchored source's 503
	// never stops the loop -- finality already bounds it.
	SourceError = "error"
)

// Publication channels (the `channel` label of bloar_follow_polls_total), the
// two of spec 8 and 8.1.
const (
	ChannelHTTPS = "https"
	ChannelIPNS  = "ipns"
)

// Embedded-host reachability states. This closed vocabulary mirrors libp2p's
// AutoNAT state without importing libp2p into the metrics package.
const (
	P2PReachabilityUnknown = "unknown"
	P2PReachabilityPrivate = "private"
	P2PReachabilityPublic  = "public"
)

// Live-peer dimensions are deliberately closed: a remote identity is never a
// metric label. Unknown connection directions are omitted, and every transport
// not covered by the three operational classes is folded into "other".
const (
	P2PDirectionInbound  = "inbound"
	P2PDirectionOutbound = "outbound"

	P2PTransportTCP   = "tcp"
	P2PTransportQUIC  = "quic"
	P2PTransportRelay = "relay"
	P2PTransportOther = "other"
)

// Bitswap peer classes are a closed operational vocabulary. Callers resolve
// precedence before recording: static > rendezvous > relay > other.
const (
	BitswapPeerStatic     = "static"
	BitswapPeerRendezvous = "rendezvous"
	BitswapPeerRelay      = "relay"
	BitswapPeerOther      = "other"
)

// Rendezvous operations and discovery-round outcomes are closed labels. A
// discovery sample is local and bounded; "available" means this node ended the
// round with at least one connected candidate, not that a global provider
// count or Sybil-resistant membership fact was established.
const (
	RendezvousOperationProvide  = "provide"
	RendezvousOperationDiscover = "discover"

	RendezvousDiscoveryAvailable = "available"
	RendezvousDiscoveryEmpty     = "empty"
	RendezvousDiscoveryTimeout   = "timeout"
)

// Exact-pointer metric dimensions mirror pointerhint's three compile-time
// semantic roles without importing that package back into metrics. Retry
// reasons distinguish a locally absent/unverified pointer, a failed local
// eligibility check, and an attempted DHT write that returned an error.
const (
	PointerKindRoot     = "root"
	PointerKindManifest = "manifest"
	PointerKindDocument = "document"

	PointerRetryIneligible   = "ineligible"
	PointerRetryCheckError   = "check_error"
	PointerRetryProvideError = "provide_error"
)

// Adoption refusals (the `reason` label of bloar_follow_refusals_total). A closed
// set of the follower's replay and consistency defences (spec 11.3, 11.4), which fire
// after a document has already resolved and so are counted by neither
// follow_polls_total nor any error path. They act at three levels: PER-HEAD
// (synced_to_floor, manifest_ancestry, coverage_mismatch, quarantined) refuse one head
// of a document; DOCUMENT-level (updated_at_floor) refuses the whole document; and
// CHANNEL-level (ipns_seq_floor) refuses an IPNS record whose replayed sequence a
// newer poll has already superseded. A refusal leaves the DOCUMENT freshness floor and
// every per-head checkpoint untouched -- but the IPNS replay floor is a channel fact,
// not a document one, and a document-level (updated_at_floor) refusal can legitimately
// coexist with the same admission having already raised that channel floor from the
// record it authenticated. Only ipns_seq_floor itself means the channel
// floor did not move.
const (
	// RefusalSyncedToFloor is a head whose published synced_to is below the
	// floor this node already served: a regressed writer, or an old archive
	// replayed at the follower (spec 11.3).
	RefusalSyncedToFloor = "synced_to_floor"
	// RefusalQuarantined is a head this node stopped serving for failing
	// verification (spec 11.4) refusing to be re-adopted from a later document.
	RefusalQuarantined = "quarantined"
	// RefusalManifestAncestry is a head whose newly published manifest tip does
	// not walk back through the tip this node already accepted: a writer that
	// swapped the head's filter history for a freshly minted chain, refused at the
	// edge exactly as a synced_to regression is (spec 10.5, 11.3).
	RefusalManifestAncestry = "manifest_ancestry"
	// RefusalCoverageMismatch is a head whose root's derived coverage disagrees
	// with the synced_to floor it is checkpointed against: a document
	// whose root and claimed synced_to differ at adoption, or a resumed checkpoint
	// whose floor sits above the coverage its own root encodes. The head is refused,
	// never served from an inconsistent generation.
	RefusalCoverageMismatch = "coverage_mismatch"
	// RefusalUpdatedAtFloor is a document dated before the global freshness floor,
	// caught when the floor is re-checked under the transition lock rather than only
	// at resolve time: a concurrent poll's newer document raised the floor after this
	// one resolved, and admitting the older one now would overwrite a newer per-head
	// checkpoint or lower the floor. Unlike the other reasons
	// this refuses the WHOLE document, not one head.
	RefusalUpdatedAtFloor = "updated_at_floor"
	// RefusalIPNSSeqFloor is a winning IPNS document whose record sequence is below
	// the replay floor, caught when the sequence is re-checked under the transition
	// lock: a concurrent poll's newer record lifted the floor past this one after it
	// resolved. A stale sequence neither lowers the floor nor
	// admits its document.
	RefusalIPNSSeqFloor = "ipns_seq_floor"
	// RefusalHandoffBlocked is a complete authenticated document whose global
	// mutable window begins beyond a configured filtered finalized frontier. The
	// whole document is refused so GC cannot open a slot neither retained head
	// covers while an exact-hash overlay is serving.
	RefusalHandoffBlocked = "handoff_blocked"
)

// Metrics is the instrument set. The zero value is not usable; use New. A nil
// *Metrics is the disabled state and every method tolerates it.
type Metrics struct {
	reg *prometheus.Registry

	// Heads (spec 5, 8).
	headSyncedTo *prometheus.GaugeVec
	headDirDepth *prometheus.GaugeVec
	headCovered  *prometheus.GaugeVec
	rootSwaps    *prometheus.CounterVec
	adoptions    *prometheus.CounterVec
	quarantined  *prometheus.GaugeVec
	// indexSegments is the head's sealed segment-window count, derived from its
	// coverage. Set beside the head-position gauges above.
	indexSegments *prometheus.GaugeVec
	// Segment sizing is orthogonal to indexSegments: exact encoded block bytes
	// and sparse row/ref density, never arithmetic window extent.
	indexSegmentBytes       *prometheus.GaugeVec
	indexSegmentRows        *prometheus.GaugeVec
	indexSegmentRefs        *prometheus.GaugeVec
	indexSegmentSealedBytes *prometheus.HistogramVec
	indexSegmentSealedMax   *prometheus.GaugeVec
	indexApplyBytes         *prometheus.CounterVec
	indexNodeLimitRefusals  *prometheus.CounterVec
	indexSegmentMaxMu       sync.Mutex
	indexSegmentMax         map[string]int

	// Read API (spec 7.1).
	beaconReads             *prometheus.CounterVec
	beaconLatency           *prometheus.HistogramVec
	publicReadAdmissions    *prometheus.CounterVec
	publicReadAdmissionCost *prometheus.CounterVec

	// Local store integrity.
	storeCorruptReads *prometheus.CounterVec

	// Ingest (spec 7.2).
	ingestBlobs   prometheus.Counter
	ingestBytes   prometheus.Counter
	ingestRejects *prometheus.CounterVec
	kzgVerify     prometheus.Histogram
	storePut      prometheus.Histogram

	// Indexer (spec 10). Recorded by the bloar-index processes, not bloard; a
	// bloard scrape leaves these at zero.
	upstreamReadDuration        prometheus.Histogram
	upstreamReadBytes           prometheus.Counter
	sourceFetch                 *prometheus.CounterVec
	blockReadDuration           prometheus.Histogram
	indexRetries                *prometheus.CounterVec
	indexOutcomes               *prometheus.CounterVec
	indexLastProgress           *prometheus.GaugeVec
	indexArchiveAvailable       *prometheus.GaugeVec
	indexBlockFetchBatches      *prometheus.CounterVec
	indexBlockFetchBlocks       *prometheus.CounterVec
	indexBlockFetchDuration     *prometheus.HistogramVec
	indexBlockFetchWorkers      *prometheus.GaugeVec
	indexBlockFetchBatchSize    *prometheus.GaugeVec
	indexBlockFetchInFlight     *prometheus.GaugeVec
	indexBlockFetchReorderDepth *prometheus.GaugeVec

	// Bounded optimistic-head tracking. `head` is configured and therefore
	// bounded; roots and slots are values, never labels.
	unfinalizedSourceHead      *prometheus.GaugeVec
	unfinalizedSourceFinalized *prometheus.GaugeVec
	unfinalizedWindowStart     *prometheus.GaugeVec
	unfinalizedWindowSlots     *prometheus.GaugeVec
	unfinalizedGeneration      *prometheus.GaugeVec
	unfinalizedLastSuccess     *prometheus.GaugeVec
	unfinalizedRetries         *prometheus.CounterVec
	unfinalizedReorgs          *prometheus.CounterVec
	unfinalizedReorgDepth      *prometheus.HistogramVec

	// Pinning and GC (spec 9).
	pins          *prometheus.GaugeVec
	pinsAdded     *prometheus.CounterVec
	pinsRemoved   *prometheus.CounterVec
	reconcile     *prometheus.HistogramVec
	reconcileErrs *prometheus.CounterVec

	gcRuns           *prometheus.CounterVec
	gcActive         prometheus.Gauge
	gcPhaseActive    *prometheus.GaugeVec
	gcPhaseDuration  *prometheus.HistogramVec
	gcMarked         prometheus.Gauge
	gcScanned        prometheus.Gauge
	gcProtected      prometheus.Gauge
	gcProtectedSkips prometheus.Counter
	gcSwept          prometheus.Counter
	gcRefetched      prometheus.Counter
	gcLastSuccess    prometheus.Gauge
	gcDuration       prometheus.Histogram
	stagingPins      prometheus.Gauge
	stagingExpired   prometheus.Counter

	// Integrity scrub is deliberately separate from reachability GC. It reads
	// and CID-validates every stored object but never deletes one.
	scrubRuns        *prometheus.CounterVec
	scrubActive      prometheus.Gauge
	scrubScanned     prometheus.Gauge
	scrubBytes       prometheus.Gauge
	scrubLastSuccess prometheus.Gauge
	scrubDuration    prometheus.Histogram

	// Store growth: coarse counts of the on-disk structures,
	// refreshed at GC cadence rather than on a scrape.
	storeBlocks    prometheus.Gauge
	storeKVEntries *prometheus.GaugeVec

	// Distribution (spec 11.2, 11.3).
	p2pReachability         *prometheus.GaugeVec
	p2pLivePeers            *prometheus.GaugeVec
	bitswapFetches          *prometheus.CounterVec
	bitswapBytes            prometheus.Counter
	bitswapScheduledBytes   *prometheus.CounterVec
	rendezvousActive        *prometheus.GaugeVec
	rendezvousProvides      *prometheus.CounterVec
	rendezvousLastSuccess   prometheus.Gauge
	rendezvousDiscoveries   *prometheus.CounterVec
	rendezvousSamples       prometheus.Gauge
	pointerCurrent          *prometheus.GaugeVec
	pointerProvides         *prometheus.CounterVec
	pointerRetries          *prometheus.CounterVec
	pointerLastSuccess      *prometheus.GaugeVec
	pointerScheduleUpdates  *prometheus.CounterVec
	ipnsPublicationStages   *prometheus.CounterVec
	ipnsLastSuccess         prometheus.Gauge
	edgePublicationStages   *prometheus.HistogramVec
	edgePublicationTx       *prometheus.CounterVec
	edgePublicationDuration *prometheus.HistogramVec
	edgePublicationWait     *prometheus.HistogramVec
	edgeDHTRoutingPeers     prometheus.Gauge
	edgeDHTRoutingSample    prometheus.Gauge
	edgeDHTQueryEvents      *prometheus.CounterVec
	edgeDHTQueryEventLast   *prometheus.GaugeVec
	edgeDHTLookupTerminated *prometheus.CounterVec
	edgeDHTLookupLast       *prometheus.GaugeVec
	edgeDHTLookupWaiting    *prometheus.GaugeVec
	followPolls             *prometheus.CounterVec
	followAdmissionDuration *prometheus.HistogramVec
	followAdmissionLastOK   prometheus.Gauge
	followSyncDuration      *prometheus.HistogramVec
	followSyncLastOK        *prometheus.GaugeVec
	followSyncActive        prometheus.Gauge
	followSyncCoalesced     prometheus.Counter
	followRefusals          *prometheus.CounterVec
	followFloorLag          *prometheus.GaugeVec
	followReady             *prometheus.GaugeVec

	// Source-set rollout telemetry is bounded by the locally configured
	// authorization relation. Publication contents can update values only for
	// those exact source/head cells; URLs, keys, CIDs, revisions, peers, and
	// errors never become labels.
	followSourceAvailable   *prometheus.GaugeVec
	followSourceLastSuccess *prometheus.GaugeVec
	followSourceCovered     *prometheus.GaugeVec
	followSourceSyncedTo    *prometheus.GaugeVec
	followSourceSelected    *prometheus.GaugeVec
	followSourceLabelsMu    sync.RWMutex
	followSources           map[string]struct{}
	followSourceCells       map[followSourceHeadCell]struct{}

	// Multi-writer conflict telemetry is config-bounded twice: the vectors use
	// only head/source labels, and observations are admitted only for cells the
	// caller registered with ConfigureFollowConflictMetrics at startup. This
	// keeps source IDs bounded by local policy and prevents evidence details
	// (CIDs, revisions, peers, URLs, or errors) from reaching Prometheus labels.
	followConflictActive      *prometheus.GaugeVec
	followConflicts           *prometheus.CounterVec
	followIncomparableActive  *prometheus.GaugeVec
	followIncomparables       *prometheus.CounterVec
	followConflictLabelsMu    sync.RWMutex
	followConflictHeads       map[string]struct{}
	followConflictSourceCells map[followConflictSourceCell]struct{}
}

type followConflictSourceCell struct {
	head   string
	source string
}

type followSourceHeadCell struct {
	head   string
	source string
}

// New returns a Metrics over a fresh registry, with the Go runtime and process
// collectors registered alongside bloar's own.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg, indexSegmentMax: make(map[string]int)}

	factory := func(o prometheus.Opts) prometheus.Opts { o.Namespace = namespace; return o }
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts(factory(prometheus.Opts{Name: name, Help: help})), labels)
	}
	gauge := func(name, help string, labels ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts(factory(prometheus.Opts{Name: name, Help: help})), labels)
	}

	m.headSyncedTo = gauge("head_synced_to", "The head's synced_to slot (spec 5.1). Absent until the head covers anything.", "head")
	m.headDirDepth = gauge("head_dir_depth", "The head's directory depth (spec 3.3).", "head")
	m.headCovered = gauge("head_covered", "1 if the head covers any slot yet, 0 if it is empty.", "head")
	m.rootSwaps = counter("head_root_swaps_total",
		"Roots that became current for the head on this node (spec 5, 8). A writer's are its own mutations; "+
			"a follower's are its adoptions, which head_adoptions_total counts too.", "head")
	m.adoptions = counter("head_adoptions_total", "Roots adopted from a publication document for the head (spec 11.3).", "head")
	m.quarantined = gauge("head_quarantined", "1 if the head is quarantined and no longer served (spec 11.4).", "head")
	m.indexSegments = gauge("index_segments",
		"Sealed segment windows in the head's directory, derived from synced_to (includes empty windows).", "head")
	m.indexSegmentBytes = gauge("index_segment_encoded_bytes",
		"Exact canonical DAG-CBOR bytes of the current open or most recently sealed Segment observed by this writer.", "head", "state")
	m.indexSegmentRows = gauge("index_segment_rows",
		"Sparse blob-carrying rows in the current open or most recently sealed Segment observed by this writer.", "head", "state")
	m.indexSegmentRefs = gauge("index_segment_refs",
		"Blob references in the current open or most recently sealed Segment observed by this writer.", "head", "state")
	m.indexSegmentSealedBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "index_segment_sealed_encoded_bytes",
		Help:      "Exact canonical DAG-CBOR bytes of non-empty Segments when their windows seal.",
		Buckets:   []float64{64 << 10, 128 << 10, 256 << 10, 512 << 10, 768 << 10, 1 << 20, 5 << 18, 3 << 19, 7 << 18, 2 << 20},
	}, []string{"head"})
	m.indexSegmentSealedMax = gauge("index_segment_sealed_max_encoded_bytes",
		"Largest exact sealed Segment observed for the head during this process lifetime.", "head")
	m.indexApplyBytes = counter("index_apply_encoded_bytes_total",
		"Exact DAG-CBOR index bytes submitted by successful ApplyRefs calls, excluding blob payload bytes.", "head")
	m.indexNodeLimitRefusals = counter("index_node_limit_refusals_total",
		"Writer mutations refused because a newly encoded index node exceeded the supported per-node limit.", "head", "node")

	m.beaconReads = counter("beacon_reads_total",
		"Responses served by the beacon-compatible read API, by head and HTTP status class (spec 7.1).", "head", "status")
	m.beaconLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "beacon_read_duration_seconds",
		Help:      "Time to serve one beacon-compatible read, by head (spec 7.1).",
		// Nitro syncs one slot at a time, serially, so this is the metric that
		// gates sync speed. The low buckets are a local blockstore read; the
		// high ones are a follower fetching over bitswap (spec 11.4).
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"head"})
	m.publicReadAdmissions = counter("public_read_admissions_total",
		"Public GET requests considered by weighted admission, by its closed outcome set.", "outcome")
	m.publicReadAdmissionCost = counter("public_read_admission_cost_total",
		"Weighted public-read cost units considered by admission, by its closed outcome set.", "outcome")
	m.storeCorruptReads = counter("store_corrupt_reads_total",
		"Local block reads that failed CID validation on the public read path, by head. The stored bytes "+
			"no longer hash to the CID they were requested under -- disk corruption, or a block altered in place -- and the "+
			"read was refused with a 500 rather than serving bytes under a CID they do not match. Any nonzero value means "+
			"this node holds a corrupt block a live root references: run `bloard fsck` to find and repair it.", "head")

	m.ingestBlobs = prometheus.NewCounter(prometheus.CounterOpts(factory(prometheus.Opts{
		Name: "ingest_blobs_total", Help: "Blobs accepted by POST /bloar/v1/blobs (spec 7.2)."})))
	m.ingestBytes = prometheus.NewCounter(prometheus.CounterOpts(factory(prometheus.Opts{
		Name: "ingest_bytes_total", Help: "Blob bytes accepted by POST /bloar/v1/blobs."})))
	m.ingestRejects = counter("ingest_rejects_total", "Bodies refused by the ingest pipeline, by reason (spec 7.2).", "reason")
	m.kzgVerify = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "ingest_kzg_verify_duration_seconds",
		Help:      "Time to compute one blob's KZG commitment and versioned hash (spec 1).",
		Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25},
	})
	m.storePut = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "store_put_duration_seconds",
		Help: "Time to write one blob's block to the flatfs blockstore during ingest (spec 7.2's store pass). " +
			"The blockstore write only: it is the disk-bound one block file per blob (spec 6), and what backs up " +
			"when ingest is IO-bound. The catalog's small KV write is not timed here.",
		// A flatfs file write, usually sub-millisecond, but the high buckets are
		// where a stalling disk or an fsync backlog shows up -- which is the thing
		// this histogram exists to catch.
		Buckets: []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	})

	m.upstreamReadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "upstream_read_duration_seconds",
		Help: "Time for the indexer to fetch one slot's blobs from its upstream (spec 10.1), including retries. " +
			"Every answered fetch is timed -- a slot with blobs, an empty or pruned slot the upstream 404s, and a " +
			"not-yet-covered slot it 503s -- because all three are round trips the indexer's throughput is bounded by.",
		// One request to a beacon node or another archive: a local node's empty
		// slot is milliseconds, 21 blobs of hex off a remote archive with a retry
		// is seconds. The buckets reach the client's 60s per-request timeout.
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	})
	m.upstreamReadBytes = prometheus.NewCounter(prometheus.CounterOpts(factory(prometheus.Opts{
		Name: "upstream_read_bytes_total",
		Help: "Blob bytes the indexer read from its upstream (spec 10.1). A slot the upstream reported empty, pruned, " +
			"or not yet covered adds nothing: those are timed by upstream_read_duration_seconds but carry no blobs."})))
	m.sourceFetch = counter("index_source_fetch_total",
		"Anchored blob-source fetches the beacon indexer made, by which source in its ordered list answered and what "+
			"it did with the slot (spec 10.1). Anchored mode derives a slot's expected versioned hashes from the trusted "+
			"block feed, then tries each source in order: \"anchored\" returned exactly those blobs, each committing to "+
			"its expected vh, and records the slot; \"mismatch\" is a source whose bytes did not commit to the expected "+
			"vh; \"absent\" is a 404 or a wrong-count 200; \"error\" is a terminal failure or a 503. Anything but "+
			"\"anchored\" moves to the next source, and all sources exhausted fails the batch -- absence is never "+
			"recorded from a blob source. Left at zero in mirror mode.",
		"source", "outcome")
	m.blockReadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "index_block_read_duration_seconds",
		Help: "Time for the beacon indexer's anchored mode to read one slot's header or blob commitments from the " +
			"trusted block feed (spec 10.1). These reads are the sole authority on what a slot contains -- existence, " +
			"absence, and the expected versioned hashes -- and they bound every anchored slot's resolution before any " +
			"blob source is asked. Left at zero in mirror mode, which reads no block feed.",
		// One request to a beacon node: a local node's header is milliseconds, a
		// blinded block with many commitments off a busy node with a retry is
		// seconds. The buckets reach the client's 60s per-request timeout.
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	})
	m.indexRetries = counter("index_retries_total",
		"Finalized-indexer loops retained and retried in-process after an explicitly classified transient. "+
			"`head` is configured and bounded; `reason` is a closed vocabulary.",
		"head", "reason")
	m.indexBlockFetchBatches = counter("index_l1_block_fetch_batches_total",
		"Blob-txs L1 full-block fetch chunks, by configured head, closed transport mode, and outcome.",
		"head", "mode", "outcome")
	m.indexBlockFetchBlocks = counter("index_l1_block_fetch_blocks_total",
		"Blob-txs L1 full blocks requested, by configured head and closed transport mode.",
		"head", "mode")
	m.indexBlockFetchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "index_l1_block_fetch_duration_seconds",
		Help: "Duration of one blob-txs full-block fetch chunk, by configured head and closed transport mode. " +
			"Batch mode is one JSON-RPC batch; fallback mode is serial BlockByNumber calls inside one bounded worker.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"head", "mode"})
	m.indexBlockFetchWorkers = gauge("index_l1_block_fetch_workers",
		"Effective bounded blob-txs full-block worker count for the configured head.", "head")
	m.indexBlockFetchBatchSize = gauge("index_l1_block_fetch_batch_size",
		"Effective maximum consecutive blocks in one blob-txs fetch chunk for the configured head.", "head")
	m.indexBlockFetchInFlight = gauge("index_l1_block_fetch_in_flight",
		"Blob-txs full-block chunks currently being fetched for the configured head.", "head")
	m.indexBlockFetchReorderDepth = gauge("index_l1_block_fetch_reorder_depth",
		"Completed blob-txs chunks waiting for earlier L1 blocks before canonical ordered reduction.", "head")
	m.indexOutcomes = counter("index_outcomes_total",
		"Finalized-indexer loop outcomes. `progress` means a durable refs batch advanced coverage, `caught_up` means "+
			"the pass found no finalized work, `retry` means an explicitly classified transient was retained for "+
			"in-process retry, and `fatal` means an unclassified or safety failure terminated the run. Both labels "+
			"have bounded configured or closed vocabularies.",
		"head", "outcome")
	m.indexLastProgress = gauge("index_last_progress_timestamp_seconds",
		"Unix timestamp of the last durable coverage advance by this beacon indexer. It is not updated by caught-up "+
			"polls or retries, so a flat value exposes a live-but-stalled finalized indexer.",
		"head")
	m.indexArchiveAvailable = gauge("index_archive_available",
		"1 when the indexer's most recently completed logical archive request received a decoded 200 success or non-5xx error response, "+
			"0 after transport failure, malformed/truncated success, or HTTP 5xx exhausted that request's bounded retries. "+
			"Intermediate retry attempts and caller cancellation do not change the gauge. A later successful request restores 1. "+
			"Authentication, configuration, and conflict 4xx responses remain fatal but count as reachable.",
		"head")
	m.unfinalizedSourceHead = gauge("unfinalized_source_head_slot",
		"Canonical optimistic source slot in the most recent complete selected snapshot.", "head")
	m.unfinalizedSourceFinalized = gauge("unfinalized_source_finalized_slot",
		"Finalized source slot authenticated by the most recent complete selected optimistic snapshot.", "head")
	m.unfinalizedWindowStart = gauge("unfinalized_window_start_slot",
		"Oldest slot retained by the most recent complete selected optimistic generation.", "head")
	m.unfinalizedWindowSlots = gauge("unfinalized_window_slots",
		"Inclusive width of the most recent complete selected optimistic generation.", "head")
	m.unfinalizedGeneration = gauge("unfinalized_generation",
		"Local mutable generation selected by the archive writer for this optimistic head.", "head")
	m.unfinalizedLastSuccess = gauge("unfinalized_last_success_timestamp_seconds",
		"Unix time when the optimistic tracker last proved and selected, or confirmed, a complete canonical snapshot.", "head")
	m.unfinalizedRetries = counter("unfinalized_retries_total",
		"Expected transient optimistic-tracker races and archive outages retained and retried in-process, by bounded reason.",
		"head", "reason")
	m.unfinalizedReorgs = counter("unfinalized_reorgs_total",
		"Canonical reorgs observed between overlapping complete optimistic snapshots.", "head")
	m.unfinalizedReorgDepth = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "unfinalized_reorg_depth_slots",
		Help: "Observed canonical reorg depth in slots from the previous tip to the newest common retained ancestor. " +
			"The top bucket also receives the retained-window lower bound when the common ancestor is deeper than both snapshots.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 96, 128, 256, 512, 1024, 2048, 4096},
	}, []string{"head"})

	m.pins = gauge("pins", "Ledger rows held, by head and purpose (spec 6.2, 9). The reserved staging head appears as head=\"_staging\".", "head", "purpose")
	m.pinsAdded = counter("pins_added_total", "Pins added by reconciliation, by head (spec 9).", "head")
	m.pinsRemoved = counter("pins_removed_total", "Pins removed by reconciliation, by head (spec 9).", "head")
	m.reconcile = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "pin_reconcile_duration_seconds",
		Help:      "Time for one head's reconciliation pass (spec 9).",
		Buckets:   []float64{.001, .01, .05, .1, .5, 1, 5, 15, 60},
	}, []string{"head"})
	m.reconcileErrs = counter("pin_reconcile_errors_total", "Reconciliation passes that failed, by head (spec 9).", "head")

	m.gcRuns = counter("gc_runs_total", "GC runs, by outcome (spec 9).", "outcome")
	m.gcActive = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "gc_active", Help: "1 while an online reachability GC run is active, otherwise 0."})))
	m.gcPhaseActive = gauge("gc_phase_active", "1 while the named bounded GC phase is active, otherwise 0.", "phase")
	m.gcPhaseDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "gc_phase_duration_seconds",
		Help:      "Time spent in each phase of online reachability GC.",
		Buckets:   []float64{.001, .01, .1, 1, 10, 60, 300, 900, 1800, 3600, 7200},
	}, []string{"phase"})
	m.gcMarked = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "gc_marked_blocks", Help: "Blocks the last GC run marked live (spec 9)."})))
	m.gcScanned = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "gc_scanned_blocks", Help: "Objects observed by the active or most recent GC sweep attempt. The online enumeration is not a point-in-time store census."})))
	m.gcProtected = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "gc_protected_blocks", Help: "Distinct block multihashes protected by application activity during the active or most recent online GC sweep attempt."})))
	m.gcProtectedSkips = prometheus.NewCounter(prometheus.CounterOpts(factory(prometheus.Opts{
		Name: "gc_protected_skips_total", Help: "Sweep candidates retained because application activity protected their multihash during the active GC epoch."})))
	m.gcSwept = prometheus.NewCounter(prometheus.CounterOpts(factory(prometheus.Opts{
		Name: "gc_swept_blocks_total", Help: "Blocks deleted by GC (spec 9)."})))
	m.gcRefetched = prometheus.NewCounter(prometheus.CounterOpts(factory(prometheus.Opts{
		Name: "gc_refetched_blocks_total",
		Help: "Missing pinned blocks GC's mark fetched to repair a dangling pin (spec 9's follower self-heal). " +
			"Zero on a writer; a nonzero rate on a follower means dangling pins are being created -- investigate."})))
	m.gcLastSuccess = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "gc_last_success_timestamp_seconds",
		Help: "Unix time of the last GC run that completed without error (spec 9). No movement for 3x gc_interval " +
			"means GC has stopped succeeding, whether or not a run is still failing loudly."})))
	m.gcDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "gc_duration_seconds",
		Help:      "Time for one GC run, including the reconciliation flush (spec 9).",
		// A sweep enumerates every block in the store, so a large archive is
		// minutes. The buckets go to an hour because a run that takes longer
		// than gc_interval is the thing an operator most needs to see.
		Buckets: []float64{1, 10, 30, 60, 300, 900, 1800, 3600},
	})
	m.stagingPins = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "staging_pins",
		Help: "Staging pins held at the last GC run: blobs an ingest accepted but nobody references yet, or blocks a " +
			"follower fetch pass fetched but has not pinned yet (spec 9)."})))
	m.stagingExpired = prometheus.NewCounter(prometheus.CounterOpts(factory(prometheus.Opts{
		Name: "staging_expired_total", Help: "Staging pins dropped for exceeding ingest.staging_ttl: abandoned puts (spec 9)."})))

	m.scrubRuns = counter("scrub_runs_total", "Full block-integrity scrub runs, by outcome.", "outcome")
	m.scrubActive = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "scrub_active", Help: "1 while a full block-integrity scrub is active, otherwise 0."})))
	m.scrubScanned = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "scrub_scanned_blocks", Help: "Blocks CID-validated by the most recently completed integrity scrub."})))
	m.scrubBytes = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "scrub_validated_bytes", Help: "Block bytes CID-validated by the most recently completed integrity scrub."})))
	m.scrubLastSuccess = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "scrub_last_success_timestamp_seconds", Help: "Unix time of the last full integrity scrub that completed without error."})))
	m.scrubDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "scrub_duration_seconds",
		Help:      "Time for one full block-integrity scrub.",
		Buckets:   []float64{1, 10, 30, 60, 300, 900, 1800, 3600, 7200, 14400},
	})

	m.storeBlocks = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "store_blocks",
		Help: "Objects observed remaining in the last GC sweep (raw blobs + dag-cbor index nodes). Online concurrent additions may not appear until a later sweep."})))
	m.storeKVEntries = gauge("store_kv_entries",
		"Pebble KV rows by prefix, measured at the last GC run (prefix=catalog ≈ distinct blob count).", "prefix")

	m.p2pReachability = gauge("p2p_reachability",
		"Current embedded-host AutoNAT reachability as a one-hot gauge over a closed state set.", "state")
	m.p2pLivePeers = gauge("p2p_live_peers",
		"Remote peers with at least one live libp2p connection in this direction/transport cell. A peer with connections in multiple cells appears in each; multiple connections in one cell count once.",
		"direction", "transport")
	m.bitswapFetches = counter("bitswap_fetches_total", "Blocks fetched over bitswap, by outcome (spec 11.2).", "outcome")
	m.bitswapBytes = prometheus.NewCounter(prometheus.CounterOpts(factory(prometheus.Opts{
		Name: "bitswap_fetched_bytes_total", Help: "Block bytes fetched over bitswap (spec 11.2)."})))
	m.bitswapScheduledBytes = counter("bitswap_scheduled_bytes_total",
		"Raw block payload bytes scheduled in outbound Bitswap envelopes, by bounded peer class. Boxo traces before network send, so this is attempted payload, not delivery confirmation; wantlists and HAVE/DONT_HAVE metadata are excluded.",
		"peer_class")
	m.rendezvousActive = gauge("rendezvous_active",
		"1 while the local rendezvous service has the named operation enabled, otherwise 0. This is configuration/lifecycle state, not network success.",
		"operation")
	m.rendezvousProvides = counter("rendezvous_provides_total",
		"Synthetic rendezvous-key DHT Provide calls completed by this node, by closed outcome. One round may call Provide once per configured unique rendezvous target.",
		"outcome")
	m.rendezvousLastSuccess = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "rendezvous_provide_last_success_timestamp_seconds",
		Help: "Unix time of the last successful synthetic rendezvous-key DHT Provide call on this node. A success confirms a local RPC result, not remote propagation or global discoverability.",
	})))
	m.rendezvousDiscoveries = counter("rendezvous_discovery_rounds_total",
		"Bounded rendezvous discovery rounds completed by local outcome: available ended with at least one connected candidate, empty completed without one, and timeout exhausted the round deadline.",
		"outcome")
	m.rendezvousSamples = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "rendezvous_observed_provider_samples",
		Help: "Provider records consumed in the most recent bounded local rendezvous discovery round. This is a capped query sample, not global provider cardinality and not a count of honest or reachable peers.",
	})))
	m.pointerCurrent = gauge("pointer_current",
		"1 when the exact-pointer provider currently has one or more pointers of this closed semantic kind, otherwise 0. A CID is deliberately never a label.",
		"kind")
	m.pointerProvides = counter("pointer_provides_total",
		"Exact-current-pointer DHT Provide calls completed by semantic kind and closed outcome. Success is a local RPC result, not proof of remote propagation.",
		"kind", "outcome")
	m.pointerRetries = counter("pointer_retries_total",
		"Retries scheduled by the exact-pointer provider, by semantic kind and closed local reason.",
		"kind", "reason")
	m.pointerLastSuccess = gauge("pointer_provide_last_success_timestamp_seconds",
		"Oldest Unix time of the last successful DHT Provide across every current pointer of this semantic kind. It is 0 until all current pointers have succeeded; withdrawal recomputes it and complete withdrawal resets it. Age is local publication freshness, not remote availability.",
		"kind")
	m.pointerScheduleUpdates = counter("pointer_schedule_updates_total",
		"Authenticated publication snapshots accepted or rejected by the edge's auxiliary exact-pointer scheduler, by closed outcome. A failure does not reverse an already-successful load-bearing IPNS transaction.",
		"outcome")
	m.ipnsPublicationStages = counter("ipns_publication_stage_total",
		"Load-bearing IPNS publication stages completed by closed stage and outcome. A successful put_record follows a successful provide_document in the same ordered transaction.",
		"stage", "outcome")
	m.ipnsLastSuccess = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "ipns_publication_last_success_timestamp_seconds",
		Help: "Unix time of the last complete provider-before-IPNS publication transaction. It never resets when a newer document becomes pending.",
	})))
	m.edgePublicationStages = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "edge_publication_stage_duration_seconds",
		Help: "Duration of each public-edge DHT stage by closed stage and outcome. provide_document precedes " +
			"put_record; timeout is the edge-owned transaction deadline, canceled is process/request shutdown, " +
			"and error is every other local DHT failure.",
		// The edge transaction budget is two minutes by default. Sub-second
		// buckets retain the normal case while the upper buckets make budget
		// tuning possible from measured DHT tails rather than inferred HTTP
		// timeouts.
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 40, 60, 90, 120},
	}, []string{"stage", "outcome"})
	m.edgePublicationTx = counter("edge_publication_transactions_total",
		"Public-edge publication transactions completed by closed operation, terminal stage, and authoritative outcome. "+
			"Identifiers and error text are logged rather than used as labels.",
		"operation", "stage", "outcome")
	m.edgePublicationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "edge_publication_transaction_duration_seconds",
		Help: "End-to-end public-edge publication transaction duration by closed operation and outcome. " +
			"The timer starts before the shared transaction permit is acquired, so bounded admission and DHT work are included.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 40, 60, 90, 120, 140},
	}, []string{"operation", "outcome"})
	m.edgePublicationWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "edge_publication_wait_duration_seconds",
		Help: "Time spent waiting for the public edge's serialized publication permit by closed operation and outcome. " +
			"Admission has its own bound and does not consume the subsequent DHT work budget.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 40, 60, 90, 120},
	}, []string{"operation", "outcome"})
	m.edgeDHTRoutingPeers = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "edge_dht_routing_table_peers",
		Help: "Amino DHT routing-table peers sampled at the most recent edge publication transaction boundary. " +
			"Use the companion sample timestamp to distinguish a current empty table from startup or stale telemetry.",
	})))
	m.edgeDHTRoutingSample = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "edge_dht_routing_table_sample_timestamp_seconds",
		Help: "Unix time when edge_dht_routing_table_peers was last sampled at a publication transaction boundary.",
	})))
	m.edgeDHTQueryEvents = counter("edge_dht_query_events_total",
		"Identity-free go-libp2p query events observed inside each public-edge DHT publication stage. "+
			"value_rpc is emitted only by PutValue, before one final peer RPC; it proves closest-peer discovery "+
			"completed, not that the remote peer stored the record.",
		"stage", "event")
	m.edgeDHTQueryEventLast = gauge("edge_dht_query_event_last_timestamp_seconds",
		"Unix time when the public edge most recently observed this closed go-libp2p query event inside the "+
			"given publication stage. Every stage/event series is materialized at zero so an unobserved event "+
			"is distinguishable from absent telemetry.",
		"stage", "event")
	m.edgeDHTLookupTerminated = counter("edge_dht_lookup_terminations_total",
		"Logical go-libp2p lookup terminations observed inside each public-edge DHT publication stage, by closed reason. "+
			"Termination may precede return from the routing call.",
		"stage", "reason")
	m.edgeDHTLookupLast = gauge("edge_dht_lookup_termination_last_timestamp_seconds",
		"Unix time when the public edge most recently observed logical go-libp2p lookup termination inside the "+
			"given publication stage. Every closed stage/reason series is materialized at zero.",
		"stage", "reason")
	m.edgeDHTLookupWaiting = gauge("edge_dht_lookup_waiting_at_last_termination",
		"Exact number of lookup peers still waiting when the most recent logical go-libp2p lookup termination was "+
			"observed inside the given publication stage. Every closed stage/reason series is materialized at zero.",
		"stage", "reason")
	m.followPolls = counter("follow_polls_total",
		"Publication-document resolutions, by channel and outcome (spec 8, 11.3). Each poll asks every configured "+
			"channel, so a follower on both contributes one sample to each. \"ok\" judges the document, not its "+
			"heads: it decoded, was for this network, verified against the followed key, and was not a replay of one "+
			"already accepted (its updated_at or IPNS sequence had not gone backwards). \"error\" is every other "+
			"outcome, from an unreachable writer to a bad signature -- the log has which. A document can resolve \"ok\" "+
			"and still have a head refused when it is adopted; follow_refusals_total counts those.",
		"channel", "outcome")
	m.followAdmissionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "follow_admission_duration_seconds",
		Help: "Wall time of one authenticated publication resolve-and-admit cycle, by closed outcome. This excludes " +
			"retained-closure sync, so it measures whether the configured poll cadence can actually keep the served registry current.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 40, 60, 90, 120},
	}, []string{"outcome"})
	m.followAdmissionLastOK = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "follow_admission_last_success_timestamp_seconds",
		Help: "Unix time when the last complete publication resolve-and-admit cycle returned successfully. It is 0 " +
			"before the first success and is independent of retained-closure sync progress.",
	})))
	m.followSyncDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "follow_sync_duration_seconds",
		Help: "Wall time of one retained-closure sync attempt for a configured head, by closed outcome. completed " +
			"means the snapshotted root/tip was CAS-stamped fetched; noop means no work was pending; superseded means " +
			"a newer adoption won while the pass ran; error is a failed pass.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 40, 60, 90, 120, 300, 600, 1800, 3600},
	}, []string{"head", "outcome"})
	m.followSyncLastOK = gauge("follow_sync_last_success_timestamp_seconds",
		"Unix time when retained-closure sync last CAS-stamped this configured head's snapshotted root/tip complete. "+
			"No-op and superseded passes do not advance it.",
		"head")
	m.followSyncActive = prometheus.NewGauge(prometheus.GaugeOpts(factory(prometheus.Opts{
		Name: "follow_sync_active",
		Help: "1 while the follower's single retained-closure worker holds the sync permit, otherwise 0. Direct Poll " +
			"calls share the same permit and cannot overlap it.",
	})))
	m.followSyncCoalesced = prometheus.NewCounter(prometheus.CounterOpts(factory(prometheus.Opts{
		Name: "follow_sync_coalesced_total",
		Help: "Sync wakeups folded into the already-pending dirty bit while the single retained-closure worker was busy. " +
			"Growth proves publication revisions are being coalesced rather than queued or spawned per revision.",
	})))
	m.followRefusals = counter("follow_refusals_total",
		"Refusals in the follower's adoption path, by reason (spec 11.3, 11.4). These fire after a document has "+
			"resolved \"ok\" -- the defences run when the document is admitted, not when it is judged at resolution -- "+
			"so follow_polls_total does not see them and no error path counts them either. The per-head reasons refuse "+
			"one head of a document: \"synced_to_floor\" is a head whose published synced_to is below the floor this "+
			"node already served (a regressed writer, or an old archive replayed at the follower); \"manifest_ancestry\" "+
			"is a head whose published manifest tip does not descend from the one this node accepted, a rewritten filter "+
			"history refused at the edge (spec 10.5); \"coverage_mismatch\" is a head whose root's derived coverage "+
			"disagrees with the synced_to it claims; \"quarantined\" is a head this node stopped serving "+
			"for failing verification (spec 11.4) refusing to be re-adopted. \"updated_at_floor\" is DOCUMENT-level: a "+
			"document dated before the freshness floor a concurrent poll raised, refusing the WHOLE document. "+
			"\"ipns_seq_floor\" is CHANNEL-level: an IPNS record whose sequence a newer poll's record already lifted the "+
			"floor past, a replay refused on the number. \"handoff_blocked\" is DOCUMENT-level: a mutable-to-finalized "+
			"handoff lacked one coherent same-publication witness, refusing the WHOLE document. Any nonzero rate is "+
			"worth an operator's attention.",
		"reason")
	m.followFloorLag = gauge("follow_synced_to_floor_lag",
		"Slots a follower is still serving as covered that the writer has retracted below its published synced_to "+
			"(spec 11.3): the head's no-regression floor minus a publication's synced_to at the moment that publication "+
			"is refused on the floor, and 0 once one is accepted. Zero means no divergence window is open. Nonzero means "+
			"the follower is serving slots the writer has retracted; it self-heals when the writer's coverage passes the "+
			"floor again.",
		"head")
	m.followReady = gauge("follow_head_ready",
		"1 once a configured followed head has been registered -- resumed from its durable checkpoint or first adopted "+
			"from a verified publication document -- and 0 before then. It is initialised to 0 for every "+
			"configured followed head at startup and returns to 0 only if the head is quarantined (spec 11.4), which takes it "+
			"out of service: a head reading 0 is one this node cannot serve -- not yet registered (a missing first-adoption "+
			"root, or a corrupt checkpoint failing closed), or quarantined -- and /readyz holds the node out of the load "+
			"balancer for exactly as long as any configured head reads 0.",
		"head")
	m.followSourceAvailable = gauge("follow_source_available",
		"1 when the most recent serialized source-set poll produced an authenticated, replay-admissible publication document from this configured source; 0 when it did not.",
		"source")
	m.followSourceLastSuccess = gauge("follow_source_last_success_timestamp_seconds",
		"Unix time when this configured source most recently produced an authenticated, replay-admissible publication document. It is 0 before the first success.",
		"source")
	m.followSourceCovered = gauge("follow_source_head_covered",
		"Whether the latest successfully observed document from this configured source covered this locally authorized head. Values are retained across source outages; pair with follow_source_available or last-success age.",
		"head", "source")
	m.followSourceSyncedTo = gauge("follow_source_head_synced_to",
		"The synced_to slot in the latest successfully observed covered claim for this locally authorized head and source. Absent when that document did not cover the head; retained across source outages.",
		"head", "source")
	m.followSourceSelected = gauge("follow_source_selected",
		"One-hot durable last-good selection provenance: 1 for the configured source attributed to this head's selected checkpoint, otherwise 0. It remains selected while sources are unavailable or the head is quarantined; pair with follow_head_ready for current serviceability.",
		"head", "source")
	m.followConflictActive = gauge("follow_conflict_active",
		"1 while a durable hard-conflict latch is active for the configured head. The last good generation remains served while advancement is frozen.",
		"head")
	m.followConflicts = counter("follow_conflicts_total",
		"Durable hard-conflict latches created for the configured head, attributed once to each configured source involved in the selected evidence pair. Repeat observations of an already-active latch do not increment it.",
		"head", "source")
	m.followIncomparableActive = gauge("follow_incomparable_active",
		"1 while the configured head's latest multi-writer arbitration result is transiently incomparable. The follower holds its last good generation and retries without creating a hard-conflict latch.",
		"head")
	m.followIncomparables = counter("follow_incomparable_total",
		"Transient incomparable multi-writer arbitration results observed for the configured head. This is an event count; follow_incomparable_active reports current state.",
		"head")

	reg.MustRegister(
		m.headSyncedTo, m.headDirDepth, m.headCovered, m.rootSwaps, m.adoptions, m.quarantined, m.indexSegments,
		m.indexSegmentBytes, m.indexSegmentRows, m.indexSegmentRefs,
		m.indexSegmentSealedBytes, m.indexSegmentSealedMax, m.indexApplyBytes, m.indexNodeLimitRefusals,
		m.beaconReads, m.beaconLatency, m.publicReadAdmissions, m.publicReadAdmissionCost, m.storeCorruptReads,
		m.ingestBlobs, m.ingestBytes, m.ingestRejects, m.kzgVerify, m.storePut,
		m.upstreamReadDuration, m.upstreamReadBytes, m.sourceFetch, m.blockReadDuration,
		m.indexRetries, m.indexOutcomes, m.indexLastProgress, m.indexArchiveAvailable,
		m.indexBlockFetchBatches, m.indexBlockFetchBlocks, m.indexBlockFetchDuration,
		m.indexBlockFetchWorkers, m.indexBlockFetchBatchSize, m.indexBlockFetchInFlight, m.indexBlockFetchReorderDepth,
		m.unfinalizedSourceHead, m.unfinalizedSourceFinalized, m.unfinalizedWindowStart,
		m.unfinalizedWindowSlots, m.unfinalizedGeneration, m.unfinalizedLastSuccess,
		m.unfinalizedRetries, m.unfinalizedReorgs, m.unfinalizedReorgDepth,
		m.pins, m.pinsAdded, m.pinsRemoved, m.reconcile, m.reconcileErrs,
		m.gcRuns, m.gcActive, m.gcPhaseActive, m.gcPhaseDuration, m.gcMarked, m.gcScanned, m.gcProtected,
		m.gcProtectedSkips, m.gcSwept, m.gcRefetched, m.gcLastSuccess, m.gcDuration, m.stagingPins, m.stagingExpired,
		m.scrubRuns, m.scrubActive, m.scrubScanned, m.scrubBytes, m.scrubLastSuccess, m.scrubDuration,
		m.storeBlocks, m.storeKVEntries,
		m.p2pReachability, m.p2pLivePeers, m.bitswapFetches, m.bitswapBytes, m.bitswapScheduledBytes,
		m.rendezvousActive, m.rendezvousProvides, m.rendezvousLastSuccess, m.rendezvousDiscoveries, m.rendezvousSamples,
		m.pointerCurrent, m.pointerProvides, m.pointerRetries, m.pointerLastSuccess, m.pointerScheduleUpdates,
		m.ipnsPublicationStages, m.ipnsLastSuccess, m.edgePublicationStages,
		m.edgePublicationTx, m.edgePublicationDuration, m.edgePublicationWait,
		m.edgeDHTRoutingPeers, m.edgeDHTRoutingSample,
		m.edgeDHTQueryEvents, m.edgeDHTQueryEventLast,
		m.edgeDHTLookupTerminated, m.edgeDHTLookupLast, m.edgeDHTLookupWaiting,
		m.followPolls, m.followAdmissionDuration, m.followAdmissionLastOK,
		m.followSyncDuration, m.followSyncLastOK, m.followSyncActive, m.followSyncCoalesced,
		m.followRefusals, m.followFloorLag, m.followReady,
		m.followSourceAvailable, m.followSourceLastSuccess, m.followSourceCovered,
		m.followSourceSyncedTo, m.followSourceSelected,
		m.followConflictActive, m.followConflicts, m.followIncomparableActive, m.followIncomparables,
	)
	for _, state := range []string{P2PReachabilityUnknown, P2PReachabilityPrivate, P2PReachabilityPublic} {
		m.p2pReachability.WithLabelValues(state).Set(0)
	}
	for _, direction := range []string{P2PDirectionInbound, P2PDirectionOutbound} {
		for _, transport := range []string{P2PTransportTCP, P2PTransportQUIC, P2PTransportRelay, P2PTransportOther} {
			m.p2pLivePeers.WithLabelValues(direction, transport).Set(0)
		}
	}
	for _, class := range []string{BitswapPeerStatic, BitswapPeerRendezvous, BitswapPeerRelay, BitswapPeerOther} {
		m.bitswapScheduledBytes.WithLabelValues(class).Add(0)
	}
	for _, operation := range []string{RendezvousOperationProvide, RendezvousOperationDiscover} {
		m.rendezvousActive.WithLabelValues(operation).Set(0)
	}
	for _, outcome := range []string{OutcomeOK, OutcomeError} {
		m.rendezvousProvides.WithLabelValues(outcome).Add(0)
	}
	for _, outcome := range []string{RendezvousDiscoveryAvailable, RendezvousDiscoveryEmpty, RendezvousDiscoveryTimeout} {
		m.rendezvousDiscoveries.WithLabelValues(outcome).Add(0)
	}
	for _, kind := range []string{PointerKindRoot, PointerKindManifest, PointerKindDocument} {
		m.pointerCurrent.WithLabelValues(kind).Set(0)
		m.pointerLastSuccess.WithLabelValues(kind).Set(0)
		for _, outcome := range []string{OutcomeOK, OutcomeError} {
			m.pointerProvides.WithLabelValues(kind, outcome).Add(0)
		}
		for _, reason := range []string{PointerRetryIneligible, PointerRetryCheckError, PointerRetryProvideError} {
			m.pointerRetries.WithLabelValues(kind, reason).Add(0)
		}
	}
	for _, outcome := range []string{OutcomeOK, OutcomeError} {
		m.pointerScheduleUpdates.WithLabelValues(outcome).Add(0)
	}
	for _, stage := range []string{IPNSStageProvideDocument, IPNSStagePutRecord} {
		for _, outcome := range []string{OutcomeOK, OutcomeError} {
			m.ipnsPublicationStages.WithLabelValues(stage, outcome).Add(0)
		}
		for _, outcome := range []string{
			OutcomeOK,
			OutcomeError,
			EdgePublicationOutcomeTimeout,
			EdgePublicationOutcomeCanceled,
		} {
			m.edgePublicationStages.WithLabelValues(stage, outcome)
		}
	}
	for _, operation := range []string{EdgePublicationOperationPublish, EdgePublicationOperationRestore} {
		for _, outcome := range []string{
			OutcomeOK,
			OutcomeError,
			EdgePublicationOutcomeTimeout,
			EdgePublicationOutcomeCanceled,
		} {
			m.edgePublicationDuration.WithLabelValues(operation, outcome)
			m.edgePublicationWait.WithLabelValues(operation, outcome)
			for _, stage := range []string{
				EdgePublicationStageAdmission,
				EdgePublicationStageValidation,
				EdgePublicationStageProvideDocument,
				EdgePublicationStagePersistState,
				EdgePublicationStagePutRecord,
				EdgePublicationStageComplete,
			} {
				m.edgePublicationTx.WithLabelValues(operation, stage, outcome).Add(0)
			}
		}
	}
	for _, stage := range []string{IPNSStageProvideDocument, IPNSStagePutRecord} {
		for _, event := range []string{
			EdgeDHTQueryEventSendingQuery,
			EdgeDHTQueryEventPeerResponse,
			EdgeDHTQueryEventQueryError,
			EdgeDHTQueryEventDialingPeer,
			EdgeDHTQueryEventValueRPC,
		} {
			m.edgeDHTQueryEvents.WithLabelValues(stage, event).Add(0)
			m.edgeDHTQueryEventLast.WithLabelValues(stage, event).Set(0)
		}
		for _, reason := range []string{
			EdgeDHTLookupTerminationStopped,
			EdgeDHTLookupTerminationCancelled,
			EdgeDHTLookupTerminationStarvation,
			EdgeDHTLookupTerminationCompleted,
		} {
			m.edgeDHTLookupTerminated.WithLabelValues(stage, reason).Add(0)
			m.edgeDHTLookupLast.WithLabelValues(stage, reason).Set(0)
			m.edgeDHTLookupWaiting.WithLabelValues(stage, reason).Set(0)
		}
	}
	for _, outcome := range []string{
		PublicReadAdmitted,
		PublicReadRejectedGlobal,
		PublicReadRejectedClient,
		PublicReadRejectedCanceled,
	} {
		m.publicReadAdmissions.WithLabelValues(outcome).Add(0)
		m.publicReadAdmissionCost.WithLabelValues(outcome).Add(0)
	}
	// Materialize every closed refusal series at zero. In particular, an alert
	// using increase(...[window]) needs a scraped zero before the first refusal;
	// otherwise a one-off first sample can appear already at 1 and has no
	// preceding sample from which Prometheus can calculate an increase.
	for _, reason := range []string{
		RefusalSyncedToFloor,
		RefusalQuarantined,
		RefusalManifestAncestry,
		RefusalCoverageMismatch,
		RefusalUpdatedAtFloor,
		RefusalIPNSSeqFloor,
		RefusalHandoffBlocked,
	} {
		m.followRefusals.WithLabelValues(reason).Add(0)
	}
	for _, phase := range []string{GCPhasePrepare, GCPhaseMark, GCPhaseSweep, GCPhaseCensus} {
		m.gcPhaseActive.WithLabelValues(phase).Set(0)
	}
	// The runtime and process collectors: goroutines, heap, GC pauses, open
	// fds, RSS. Free, standard, and the first thing anyone looks at when a
	// daemon misbehaves in a way its own metrics do not explain.
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Registry returns the registry to scrape. Nil for a nil Metrics, which is what
// lets the caller decide whether there is anything to serve.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.reg
}

// MustRegister adds a collector to the registry, so that a caller can publish
// something this package does not own -- a Pebble size gauge, say, whose source
// lives in a package that must not import Prometheus. It is a no-op when
// metrics are disabled.
func (m *Metrics) MustRegister(cs ...prometheus.Collector) {
	if m == nil {
		return
	}
	m.reg.MustRegister(cs...)
}

// P2PReachability publishes the embedded host's current AutoNAT state. Unknown
// values are ignored so a dependency change cannot create an unbounded label.
func (m *Metrics) P2PReachability(state string) {
	if m == nil {
		return
	}
	switch state {
	case P2PReachabilityUnknown, P2PReachabilityPrivate, P2PReachabilityPublic:
	default:
		return
	}
	for _, known := range []string{P2PReachabilityUnknown, P2PReachabilityPrivate, P2PReachabilityPublic} {
		value := 0.0
		if known == state {
			value = 1
		}
		m.p2pReachability.WithLabelValues(known).Set(value)
	}
}

// P2PLivePeers sets one reconciled live-peer cell. direction and transport
// must be the closed P2PDirection* and P2PTransport* constants; invalid values
// and negative counts are ignored so a dependency change cannot create a new
// label or an impossible gauge value.
func (m *Metrics) P2PLivePeers(direction, transport string, count int) {
	if m == nil || count < 0 || !validP2PDirection(direction) || !validP2PTransport(transport) {
		return
	}
	m.p2pLivePeers.WithLabelValues(direction, transport).Set(float64(count))
}

func validP2PDirection(direction string) bool {
	return direction == P2PDirectionInbound || direction == P2PDirectionOutbound
}

func validP2PTransport(transport string) bool {
	switch transport {
	case P2PTransportTCP, P2PTransportQUIC, P2PTransportRelay, P2PTransportOther:
		return true
	default:
		return false
	}
}

// HeadInfo records a head's position after a mutation or an adoption.
func (m *Metrics) HeadInfo(head string, syncedTo uint64, covered bool, dirDepth uint64) {
	if m == nil {
		return
	}
	m.headDirDepth.WithLabelValues(head).Set(float64(dirDepth))
	if covered {
		m.headCovered.WithLabelValues(head).Set(1)
		m.headSyncedTo.WithLabelValues(head).Set(float64(syncedTo))
		return
	}
	m.headCovered.WithLabelValues(head).Set(0)
}

// HeadStructure records a head's sealed segment-window count. It is
// derived arithmetically from the head's coverage (the caller computes it from
// synced_to, origin_slot and seg_bits), so it costs no DAG read; it is set at
// the same register/resume and post-mutation points as HeadInfo.
func (m *Metrics) HeadStructure(head string, segments uint64) {
	if m == nil {
		return
	}
	m.indexSegments.WithLabelValues(head).Set(float64(segments))
}

// IndexSegment records the exact bytes and sparse density of an accepted
// writer Segment. Open samples update on every successful apply; sealed
// samples additionally feed a process-lifetime histogram and maximum.
func (m *Metrics) IndexSegment(head, state string, encodedBytes, rows, refs int) {
	if !m.indexSegmentSnapshot(head, state, encodedBytes, rows, refs) {
		return
	}
	if state != IndexSegmentSealed {
		return
	}
	m.indexSegmentSealedBytes.WithLabelValues(head).Observe(float64(encodedBytes))
	m.indexSegmentMaxMu.Lock()
	if encodedBytes > m.indexSegmentMax[head] {
		m.indexSegmentMax[head] = encodedBytes
		m.indexSegmentSealedMax.WithLabelValues(head).Set(float64(encodedBytes))
	}
	m.indexSegmentMaxMu.Unlock()
}

// IndexSegmentSnapshot restores accepted open/last-sealed gauges without
// replaying a historical seal into the event histogram or process-lifetime
// maximum.
func (m *Metrics) IndexSegmentSnapshot(head, state string, encodedBytes, rows, refs int) {
	m.indexSegmentSnapshot(head, state, encodedBytes, rows, refs)
}

func (m *Metrics) indexSegmentSnapshot(head, state string, encodedBytes, rows, refs int) bool {
	if m == nil || !validIndexSegmentState(state) || encodedBytes < 0 || rows < 0 || refs < 0 {
		return false
	}
	m.indexSegmentBytes.WithLabelValues(head, state).Set(float64(encodedBytes))
	m.indexSegmentRows.WithLabelValues(head, state).Set(float64(rows))
	m.indexSegmentRefs.WithLabelValues(head, state).Set(float64(refs))
	return true
}

// IndexApply records exact encoded index DAG bytes for a successful apply.
func (m *Metrics) IndexApply(head string, encodedBytes uint64) {
	if m == nil {
		return
	}
	m.indexApplyBytes.WithLabelValues(head).Add(float64(encodedBytes))
}

// IndexNodeLimitRefusal makes fail-closed publication observable even though
// accepted-state gauges deliberately remain on the prior generation.
func (m *Metrics) IndexNodeLimitRefusal(head, node string) {
	if m == nil || !validIndexNode(node) {
		return
	}
	m.indexNodeLimitRefusals.WithLabelValues(head, node).Inc()
}

func validIndexSegmentState(state string) bool {
	return state == IndexSegmentOpen || state == IndexSegmentSealed
}

func validIndexNode(node string) bool {
	return node == IndexNodeHead || node == IndexNodeDir || node == IndexNodeSegment
}

// StoreBlocks records the objects observed remaining by the last GC sweep
// as scanned-minus-swept, with no extra walk. Online enumeration is
// deliberately not a point-in-time snapshot, so concurrent additions may be
// absent until the next run; this is a trend gauge, not an exact live census.
func (m *Metrics) StoreBlocks(n int) {
	if m == nil {
		return
	}
	m.storeBlocks.Set(float64(n))
}

// KVEntry records the row count of one KV prefix as measured at the last GC run
// Prefix is a short human name (catalog, roots, manifest, ipns,
// follower), not the raw byte. The count is an O(n) key-only scan run at GC
// cadence, deliberately never on a scrape.
func (m *Metrics) KVEntry(prefix string, n uint64) {
	if m == nil {
		return
	}
	m.storeKVEntries.WithLabelValues(prefix).Set(float64(n))
}

// RootSwap counts one published root (spec 5, 8).
func (m *Metrics) RootSwap(head string) {
	if m == nil {
		return
	}
	m.rootSwaps.WithLabelValues(head).Inc()
}

// Adoption counts one root adopted from a publication document (spec 11.3).
func (m *Metrics) Adoption(head string) {
	if m == nil {
		return
	}
	m.adoptions.WithLabelValues(head).Inc()
}

// Quarantined sets the head's quarantine gauge (spec 11.4). It is a gauge and
// not a counter because quarantine is a state a head is in, and one-way for the
// life of the process: what an operator alerts on is that it is 1, not that it
// became 1 while they were not looking.
func (m *Metrics) Quarantined(head string, yes bool) {
	if m == nil {
		return
	}
	v := 0.0
	if yes {
		v = 1
	}
	m.quarantined.WithLabelValues(head).Set(v)
}

// BeaconRead records one served read of spec 7.1's blobs endpoint. status is an
// HTTP status code; it is bucketed into a class here so that the label stays
// small.
func (m *Metrics) BeaconRead(head string, status int, d time.Duration) {
	if m == nil {
		return
	}
	m.beaconReads.WithLabelValues(head, statusClass(status)).Inc()
	m.beaconLatency.WithLabelValues(head).Observe(d.Seconds())
}

// PublicReadAdmission records one weighted limiter decision. outcome is
// deliberately checked against a fixed vocabulary before touching the vector:
// an accidental URL, client address, or error string cannot create an unbounded
// Prometheus label. Invalid or negative costs are ignored as caller bugs.
func (m *Metrics) PublicReadAdmission(outcome string, cost int) {
	if m == nil || cost < 0 {
		return
	}
	switch outcome {
	case PublicReadAdmitted, PublicReadRejectedGlobal, PublicReadRejectedClient, PublicReadRejectedCanceled:
	default:
		return
	}
	m.publicReadAdmissions.WithLabelValues(outcome).Inc()
	m.publicReadAdmissionCost.WithLabelValues(outcome).Add(float64(cost))
}

// CorruptRead records one public read refused because a local block failed CID
// validation. head is the resolved head the read was for, always
// a registered name (the read path validates only after resolving the head), so
// the label is bounded.
func (m *Metrics) CorruptRead(head string) {
	if m == nil {
		return
	}
	m.storeCorruptReads.WithLabelValues(head).Inc()
}

// statusClass renders an HTTP status as its class: 200 and 206 are both "2xx".
func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

// Ingested records an accepted batch: n blobs of the given total size.
func (m *Metrics) Ingested(n int, bytes int) {
	if m == nil {
		return
	}
	m.ingestBlobs.Add(float64(n))
	m.ingestBytes.Add(float64(bytes))
}

// IngestReject counts one refused body. reason is one of the Reject constants.
func (m *Metrics) IngestReject(reason string) {
	if m == nil {
		return
	}
	m.ingestRejects.WithLabelValues(reason).Inc()
}

// KZGVerify records the time to derive one blob's versioned hash.
func (m *Metrics) KZGVerify(d time.Duration) {
	if m == nil {
		return
	}
	m.kzgVerify.Observe(d.Seconds())
}

// StorePut records the time to write one blob's block to the blockstore in
// ingest's store pass (spec 7.2). The blockstore only, not the catalog.
func (m *Metrics) StorePut(d time.Duration) {
	if m == nil {
		return
	}
	m.storePut.Observe(d.Seconds())
}

// UpstreamRead records one slot fetch from an indexer's upstream (spec 10.1):
// the time it took and the blob bytes it returned. bytes is 0 for a slot the
// upstream reported empty, pruned, or not yet covered -- a round trip the
// duration still counts.
func (m *Metrics) UpstreamRead(bytes int, d time.Duration) {
	if m == nil {
		return
	}
	m.upstreamReadBytes.Add(float64(bytes))
	m.upstreamReadDuration.Observe(d.Seconds())
}

// SourceFetch counts one anchored blob-source fetch attempt, by which source
// answered and what it did with the slot (spec 10.1). source is SourcePrimary
// or SourceFallback; outcome is one of the Source* outcome constants. Recorded
// once per source attempt, so a slot resolved by the primary touches it once and
// one that fell through to the fallback touches it twice.
func (m *Metrics) SourceFetch(source, outcome string) {
	if m == nil {
		return
	}
	m.sourceFetch.WithLabelValues(source, outcome).Inc()
}

// BlockRead records one header or commitments read from the anchored block feed
// (spec 10.1): the trusted per-slot authority anchored mode resolves against.
func (m *Metrics) BlockRead(d time.Duration) {
	if m == nil {
		return
	}
	m.blockReadDuration.Observe(d.Seconds())
}

// Closed finalized-indexer metric labels. Keeping these at the metrics boundary
// prevents a wrapped error, URL, or slot from becoming cardinality.
const (
	IndexRetryExecutionOptimistic = "execution_optimistic"
	IndexRetryArchiveUnavailable  = "archive_unavailable"

	IndexOutcomeProgress = "progress"
	IndexOutcomeCaughtUp = "caught_up"
	IndexOutcomeRetry    = "retry"
	IndexOutcomeFatal    = "fatal"

	IndexBlockFetchBatch    = "batch"
	IndexBlockFetchFallback = "fallback"
)

// IndexBlockFetch records one bounded blob-txs full-block fetch chunk. Unknown
// modes are rejected at this boundary so an RPC URL or implementation name can
// never become a metric label.
func (m *Metrics) IndexBlockFetch(head, mode string, ok bool, blocks int, d time.Duration) {
	if m == nil {
		return
	}
	switch mode {
	case IndexBlockFetchBatch, IndexBlockFetchFallback:
	default:
		return
	}
	outcome := OutcomeError
	if ok {
		outcome = OutcomeOK
	}
	m.indexBlockFetchBatches.WithLabelValues(head, mode, outcome).Inc()
	m.indexBlockFetchBlocks.WithLabelValues(head, mode).Add(float64(blocks))
	m.indexBlockFetchDuration.WithLabelValues(head, mode).Observe(d.Seconds())
}

// IndexBlockFetchConfig publishes the effective worker and chunk bounds after
// defaults. Non-positive values are ignored rather than exporting a misleading
// disabled-looking configuration.
func (m *Metrics) IndexBlockFetchConfig(head string, workers, batchSize int) {
	if m == nil || workers <= 0 || batchSize <= 0 {
		return
	}
	m.indexBlockFetchWorkers.WithLabelValues(head).Set(float64(workers))
	m.indexBlockFetchBatchSize.WithLabelValues(head).Set(float64(batchSize))
}

// IndexBlockFetchInFlight changes the current bounded fetch count by delta.
func (m *Metrics) IndexBlockFetchInFlight(head string, delta int) {
	if m == nil {
		return
	}
	m.indexBlockFetchInFlight.WithLabelValues(head).Add(float64(delta))
}

// IndexBlockFetchReorderDepth records how many completed chunks are currently
// waiting behind an earlier one. depth is state, never a label.
func (m *Metrics) IndexBlockFetchReorderDepth(head string, depth int) {
	if m == nil || depth < 0 {
		return
	}
	m.indexBlockFetchReorderDepth.WithLabelValues(head).Set(float64(depth))
}

// Closed unfinalized-tracker retry reasons. The tracker owns the classification,
// while the metrics boundary owns the label vocabulary.
const (
	UnfinalizedRetryExecutionOptimistic = "execution_optimistic"
	UnfinalizedRetryHandoffChanged      = "handoff_changed"
	UnfinalizedRetryArchiveUnavailable  = "archive_unavailable"
)

// IndexRetry records one finalized beacon-indexer transient retained for an
// in-process retry. Unknown reasons are rejected at this boundary rather than
// becoming metric labels.
func (m *Metrics) IndexRetry(head, reason string) {
	if m == nil {
		return
	}
	switch reason {
	case IndexRetryExecutionOptimistic, IndexRetryArchiveUnavailable:
	default:
		return
	}
	m.indexRetries.WithLabelValues(head, reason).Inc()
}

// IndexOutcome records one bounded indexer-loop outcome. Unknown outcomes are
// rejected rather than exported as labels.
func (m *Metrics) IndexOutcome(head, outcome string) {
	if m == nil {
		return
	}
	switch outcome {
	case IndexOutcomeProgress, IndexOutcomeCaughtUp, IndexOutcomeRetry, IndexOutcomeFatal:
		m.indexOutcomes.WithLabelValues(head, outcome).Inc()
	}
}

// IndexProgress records a durable coverage advance and stamps its freshness.
func (m *Metrics) IndexProgress(head string) {
	if m == nil {
		return
	}
	m.indexOutcomes.WithLabelValues(head, IndexOutcomeProgress).Inc()
	m.indexLastProgress.WithLabelValues(head).SetToCurrentTime()
}

// IndexArchiveAvailable records the current availability of the archive
// dependency for one configured indexer head. It is deliberately distinct from
// source availability: the archive is the indexer's durable progress and write
// authority, not the beacon/L1 source it is ingesting.
func (m *Metrics) IndexArchiveAvailable(head string, available bool) {
	if m == nil {
		return
	}
	if available {
		m.indexArchiveAvailable.WithLabelValues(head).Set(1)
		return
	}
	m.indexArchiveAvailable.WithLabelValues(head).Set(0)
}

// UnfinalizedSnapshot records one complete optimistic snapshot which the
// archive selected, or confirmed was already selected. All dimensions are
// values under one configured head label. Invalid bounds are ignored rather
// than exporting a wrapped or impossible window width.
func (m *Metrics) UnfinalizedSnapshot(head string, sourceHead, sourceFinalized, windowStart, generation uint64) {
	if m == nil || sourceFinalized > sourceHead || windowStart > sourceHead {
		return
	}
	widthMinusOne := sourceHead - windowStart
	if widthMinusOne == ^uint64(0) {
		return
	}
	m.unfinalizedSourceHead.WithLabelValues(head).Set(float64(sourceHead))
	m.unfinalizedSourceFinalized.WithLabelValues(head).Set(float64(sourceFinalized))
	m.unfinalizedWindowStart.WithLabelValues(head).Set(float64(windowStart))
	m.unfinalizedWindowSlots.WithLabelValues(head).Set(float64(widthMinusOne + 1))
	m.unfinalizedGeneration.WithLabelValues(head).Set(float64(generation))
	m.unfinalizedLastSuccess.WithLabelValues(head).SetToCurrentTime()
}

// ConfigureUnfinalizedHeadMetrics materializes the reorg-depth histogram for
// one configured mutable head before the first reorg occurs.
func (m *Metrics) ConfigureUnfinalizedHeadMetrics(head string) {
	if m == nil || head == "" {
		return
	}
	m.unfinalizedReorgDepth.WithLabelValues(head)
}

// UnfinalizedRetry records one expected transient race retained and retried by
// the optimistic tracker. reason is one of the closed constants owned by the
// tracker; arbitrary error text never becomes a label.
func (m *Metrics) UnfinalizedRetry(head, reason string) {
	if m == nil {
		return
	}
	switch reason {
	case UnfinalizedRetryExecutionOptimistic, UnfinalizedRetryHandoffChanged, UnfinalizedRetryArchiveUnavailable:
	default:
		return
	}
	m.unfinalizedRetries.WithLabelValues(head, reason).Inc()
}

// UnfinalizedReorg records one reorg proven by two overlapping canonical
// snapshots. depth is the old tip's distance from their newest common retained
// ancestor, or a retained-window lower bound when that ancestor is deeper.
func (m *Metrics) UnfinalizedReorg(head string, depth uint64) {
	if m == nil || depth == 0 {
		return
	}
	m.unfinalizedReorgs.WithLabelValues(head).Inc()
	m.unfinalizedReorgDepth.WithLabelValues(head).Observe(float64(depth))
}

// Pins publishes the ledger row count of one head and purpose.
func (m *Metrics) Pins(head, purpose string, n int) {
	if m == nil {
		return
	}
	m.pins.WithLabelValues(head, purpose).Set(float64(n))
}

// Reconciled records one head's pass.
func (m *Metrics) Reconciled(head string, added, removed int, d time.Duration) {
	if m == nil {
		return
	}
	m.pinsAdded.WithLabelValues(head).Add(float64(added))
	m.pinsRemoved.WithLabelValues(head).Add(float64(removed))
	m.reconcile.WithLabelValues(head).Observe(d.Seconds())
}

// ReconcileError counts one failed pass.
func (m *Metrics) ReconcileError(head string) {
	if m == nil {
		return
	}
	m.reconcileErrs.WithLabelValues(head).Inc()
}

// GCRun records one run. A failed run reports what it managed before it failed,
// which is nothing for a mark failure and a partial sweep otherwise. refetched
// is the dangling pins its mark healed (spec 9's follower self-heal); a
// successful run also stamps the last-success gauge, which is what an alert
// watches to notice GC has quietly stopped succeeding.
func (m *Metrics) GCRun(ok bool, marked, swept, refetched int, d time.Duration) {
	if m == nil {
		return
	}
	outcome := OutcomeError
	if ok {
		outcome = OutcomeOK
	}
	m.gcRuns.WithLabelValues(outcome).Inc()
	m.gcMarked.Set(float64(marked))
	m.gcSwept.Add(float64(swept))
	m.gcRefetched.Add(float64(refetched))
	m.gcDuration.Observe(d.Seconds())
	if ok {
		m.gcLastSuccess.SetToCurrentTime()
	}
}

// GCActive publishes whether an online collection epoch is in flight.
func (m *Metrics) GCActive(active bool) {
	if m == nil {
		return
	}
	if active {
		m.gcActive.Set(1)
	} else {
		m.gcActive.Set(0)
	}
}

// GCPhase selects one phase from the closed GC phase set. Passing an empty
// phase clears every phase. Unknown values are ignored so the label set cannot
// grow from caller input.
func (m *Metrics) GCPhase(phase string) {
	if m == nil {
		return
	}
	for _, known := range []string{GCPhasePrepare, GCPhaseMark, GCPhaseSweep, GCPhaseCensus} {
		v := 0.0
		if phase == known {
			v = 1
		}
		m.gcPhaseActive.WithLabelValues(known).Set(v)
	}
}

// GCPhaseDuration records one completed bounded GC phase.
func (m *Metrics) GCPhaseDuration(phase string, d time.Duration) {
	if m == nil {
		return
	}
	switch phase {
	case GCPhasePrepare, GCPhaseMark, GCPhaseSweep, GCPhaseCensus:
		m.gcPhaseDuration.WithLabelValues(phase).Observe(d.Seconds())
	}
}

// GCObserved publishes the online sweep's non-snapshot observation counts.
// protectedSkips is also accumulated because it is the direct signal that the
// write barrier prevented a deletion race.
func (m *Metrics) GCObserved(scanned, protected, protectedSkips int) {
	if m == nil {
		return
	}
	m.gcScanned.Set(float64(scanned))
	m.gcProtected.Set(float64(protected))
	m.gcProtectedSkips.Add(float64(protectedSkips))
}

// GCProgress updates the in-flight observation gauges without changing any
// cumulative counters. The final GCObserved call replaces these with the
// completed run's values.
func (m *Metrics) GCProgress(scanned, protected int) {
	if m == nil {
		return
	}
	m.gcScanned.Set(float64(scanned))
	m.gcProtected.Set(float64(protected))
}

// ScrubActive publishes whether a full integrity scrub is in flight.
func (m *Metrics) ScrubActive(active bool) {
	if m == nil {
		return
	}
	if active {
		m.scrubActive.Set(1)
	} else {
		m.scrubActive.Set(0)
	}
}

// ScrubProgress updates the in-flight validated-object gauges.
func (m *Metrics) ScrubProgress(scanned int, bytes int64) {
	if m == nil {
		return
	}
	m.scrubScanned.Set(float64(scanned))
	m.scrubBytes.Set(float64(bytes))
}

// ScrubRun records a completed integrity scrub. Counts are published for both
// outcomes so an operator can see how far a failed pass got; only success
// advances the freshness timestamp.
func (m *Metrics) ScrubRun(ok bool, scanned int, bytes int64, d time.Duration) {
	if m == nil {
		return
	}
	outcome := OutcomeError
	if ok {
		outcome = OutcomeOK
	}
	m.scrubRuns.WithLabelValues(outcome).Inc()
	m.scrubScanned.Set(float64(scanned))
	m.scrubBytes.Set(float64(bytes))
	m.scrubDuration.Observe(d.Seconds())
	if ok {
		m.scrubLastSuccess.SetToCurrentTime()
	}
}

// StagingPins publishes the staging row count seen by the last GC run.
func (m *Metrics) StagingPins(n int) {
	if m == nil {
		return
	}
	m.stagingPins.Set(float64(n))
}

// StagingExpired counts staging rows dropped for exceeding their TTL.
func (m *Metrics) StagingExpired(n int) {
	if m == nil {
		return
	}
	m.stagingExpired.Add(float64(n))
}

// BitswapFetch records one block fetch over bitswap. bytes is 0 on failure.
func (m *Metrics) BitswapFetch(ok bool, bytes int) {
	if m == nil {
		return
	}
	outcome := OutcomeError
	if ok {
		outcome = OutcomeOK
	}
	m.bitswapFetches.WithLabelValues(outcome).Inc()
	m.bitswapBytes.Add(float64(bytes))
}

// BitswapScheduled records raw block payload bytes in an outbound Bitswap
// envelope. Boxo invokes its tracer before attempting the network write, so
// this is scheduled/attempted payload and must not be read as delivered bytes.
// Wants and block-presence metadata contribute zero. peerClass must be one of
// the closed BitswapPeer* constants; invalid classes and negative sizes are
// ignored rather than becoming labels or decreasing a counter.
func (m *Metrics) BitswapScheduled(peerClass string, bytes int) {
	if m == nil || bytes < 0 || !validBitswapPeerClass(peerClass) {
		return
	}
	m.bitswapScheduledBytes.WithLabelValues(peerClass).Add(float64(bytes))
}

func validBitswapPeerClass(peerClass string) bool {
	switch peerClass {
	case BitswapPeerStatic, BitswapPeerRendezvous, BitswapPeerRelay, BitswapPeerOther:
		return true
	default:
		return false
	}
}

// RendezvousActive records whether one local rendezvous operation is enabled.
// operation must be one of the closed RendezvousOperation* constants.
func (m *Metrics) RendezvousActive(operation string, active bool) {
	if m == nil || !validRendezvousOperation(operation) {
		return
	}
	value := 0.0
	if active {
		value = 1
	}
	m.rendezvousActive.WithLabelValues(operation).Set(value)
}

// RendezvousProvide records one completed synthetic-key DHT Provide call. A
// successful completion stamps the supplied time, making schedule tests
// deterministic and keeping wall-clock ownership at the operation seam.
func (m *Metrics) RendezvousProvide(outcome string, completedAt time.Time) {
	if m == nil || !validOutcome(outcome) {
		return
	}
	m.rendezvousProvides.WithLabelValues(outcome).Inc()
	if outcome == OutcomeOK && !completedAt.IsZero() {
		m.rendezvousLastSuccess.Set(timestampSeconds(completedAt))
	}
}

// RendezvousDiscovery records one completed bounded local discovery round.
// observed is the number of provider channel values consumed, never an
// assertion about global provider cardinality.
func (m *Metrics) RendezvousDiscovery(outcome string, observed int) {
	if m == nil || observed < 0 || !validRendezvousDiscoveryOutcome(outcome) {
		return
	}
	m.rendezvousDiscoveries.WithLabelValues(outcome).Inc()
	m.rendezvousSamples.Set(float64(observed))
}

// PointerCurrent records whether the exact-pointer provider has a current
// pointer of kind. changed resets the last-success timestamp before a new CID
// has succeeded, so freshness can never be inherited from the replaced CID.
func (m *Metrics) PointerCurrent(kind string, current, changed bool) {
	if m == nil || !validPointerKind(kind) {
		return
	}
	if !current {
		m.pointerCurrent.WithLabelValues(kind).Set(0)
		m.pointerLastSuccess.WithLabelValues(kind).Set(0)
		return
	}
	m.pointerCurrent.WithLabelValues(kind).Set(1)
	if changed {
		m.pointerLastSuccess.WithLabelValues(kind).Set(0)
	}
}

// PointerSchedule records aggregate exact-pointer state without incrementing
// an attempt counter. oldestSuccess must be the oldest last-success time among
// every current CID of kind; its zero value means at least one current CID has
// never completed a successful Provide. Keeping this separate from
// PointerProvideOutcome prevents one successful member of a multi-head
// schedule from making the whole semantic kind appear fresh.
func (m *Metrics) PointerSchedule(kind string, current bool, oldestSuccess time.Time) {
	if m == nil || !validPointerKind(kind) {
		return
	}
	if !current {
		m.pointerCurrent.WithLabelValues(kind).Set(0)
		m.pointerLastSuccess.WithLabelValues(kind).Set(0)
		return
	}
	m.pointerCurrent.WithLabelValues(kind).Set(1)
	if oldestSuccess.IsZero() {
		m.pointerLastSuccess.WithLabelValues(kind).Set(0)
		return
	}
	m.pointerLastSuccess.WithLabelValues(kind).Set(timestampSeconds(oldestSuccess))
}

// PointerProvideOutcome records one completed exact-pointer DHT Provide call
// without asserting aggregate schedule freshness. Provider owns the per-CID
// success times and follows this call with PointerSchedule when they change.
func (m *Metrics) PointerProvideOutcome(kind, outcome string) {
	if m == nil || !validPointerKind(kind) || !validOutcome(outcome) {
		return
	}
	m.pointerProvides.WithLabelValues(kind, outcome).Inc()
}

// PointerProvide records one completed exact-pointer DHT Provide call. A
// success stamps freshness for the current pointer of kind.
func (m *Metrics) PointerProvide(kind, outcome string, completedAt time.Time) {
	if m == nil || !validPointerKind(kind) || !validOutcome(outcome) {
		return
	}
	m.PointerProvideOutcome(kind, outcome)
	if outcome == OutcomeOK && !completedAt.IsZero() {
		m.pointerLastSuccess.WithLabelValues(kind).Set(timestampSeconds(completedAt))
	}
}

// PointerRetry records one retry scheduled for a closed local reason.
func (m *Metrics) PointerRetry(kind, reason string) {
	if m == nil || !validPointerKind(kind) || !validPointerRetryReason(reason) {
		return
	}
	m.pointerRetries.WithLabelValues(kind, reason).Inc()
}

// PointerScheduleUpdate records the bounded local handoff from an authenticated
// edge publication to its auxiliary exact-pointer schedule. It is separate
// from DHT Provide attempts: a successfully installed schedule may still be
// waiting for its first asynchronous network write.
func (m *Metrics) PointerScheduleUpdate(outcome string) {
	if m == nil || !validOutcome(outcome) {
		return
	}
	m.pointerScheduleUpdates.WithLabelValues(outcome).Inc()
}

// IPNSPublicationStage records one completed stage of the load-bearing
// provider-before-IPNS transaction. Only a successful record put stamps the
// historical last-success time; a new pending document never clears it.
func (m *Metrics) IPNSPublicationStage(stage, outcome string, completedAt time.Time) {
	if m == nil || !validIPNSPublicationStage(stage) || !validOutcome(outcome) {
		return
	}
	m.ipnsPublicationStages.WithLabelValues(stage, outcome).Inc()
	if stage == IPNSStagePutRecord && outcome == OutcomeOK && !completedAt.IsZero() {
		m.ipnsLastSuccess.Set(timestampSeconds(completedAt))
	}
}

// EdgePublicationStage observes one actual DHT stage in the public edge. It is
// intentionally distinct from IPNSPublicationStage: the latter is the private
// writer's end-to-end accounting, while this histogram identifies which edge
// operation consumed the transaction budget.
func (m *Metrics) EdgePublicationStage(stage, outcome string, duration time.Duration) {
	if m == nil || !validIPNSPublicationStage(stage) || !validEdgePublicationOutcome(outcome) || duration < 0 {
		return
	}
	m.edgePublicationStages.WithLabelValues(stage, outcome).Observe(duration.Seconds())
}

// EdgePublicationTransaction records one completed publish or durable-restore
// transaction. The stage is the terminal point, not an identifier-bearing
// description of the submitted publication.
func (m *Metrics) EdgePublicationTransaction(operation, stage, outcome string, duration time.Duration) {
	if m == nil || !validEdgePublicationOperation(operation) || !validEdgePublicationStage(stage) ||
		!validEdgePublicationOutcome(outcome) || duration < 0 {
		return
	}
	m.edgePublicationTx.WithLabelValues(operation, stage, outcome).Inc()
	m.edgePublicationDuration.WithLabelValues(operation, outcome).Observe(duration.Seconds())
}

// EdgePublicationWait records how long a publish or restore waited for the
// edge's single serialized transaction permit.
func (m *Metrics) EdgePublicationWait(operation, outcome string, duration time.Duration) {
	if m == nil || !validEdgePublicationOperation(operation) ||
		!validEdgePublicationOutcome(outcome) || duration < 0 {
		return
	}
	m.edgePublicationWait.WithLabelValues(operation, outcome).Observe(duration.Seconds())
}

// EdgeDHTRoutingTablePeers records a bounded scalar snapshot at a publication
// transaction boundary. Peer identities never enter metrics.
func (m *Metrics) EdgeDHTRoutingTablePeers(peers int, observedAt time.Time) {
	if m == nil || peers < 0 {
		return
	}
	m.edgeDHTRoutingPeers.Set(float64(peers))
	if !observedAt.IsZero() {
		m.edgeDHTRoutingSample.Set(timestampSeconds(observedAt))
	}
}

// EdgeDHTQueryEvent records one bounded go-libp2p query event without peer
// identity, address, key, record, or error-text labels.
func (m *Metrics) EdgeDHTQueryEvent(stage, event string, observedAt time.Time) {
	if m == nil || !validIPNSPublicationStage(stage) || !validEdgeDHTQueryEvent(event) {
		return
	}
	m.edgeDHTQueryEvents.WithLabelValues(stage, event).Inc()
	if !observedAt.IsZero() {
		m.edgeDHTQueryEventLast.WithLabelValues(stage, event).Set(timestampSeconds(observedAt))
	}
}

// EdgeDHTLookupTermination records one logical lookup termination and the
// exact number of peers still in Waiting state at that instant. It deliberately
// accepts no lookup identifier, peer identity, key, address, or error detail.
func (m *Metrics) EdgeDHTLookupTermination(stage, reason string, waiting int, observedAt time.Time) {
	if m == nil || !validIPNSPublicationStage(stage) ||
		!validEdgeDHTLookupTermination(reason) || waiting < 0 {
		return
	}
	m.edgeDHTLookupTerminated.WithLabelValues(stage, reason).Inc()
	m.edgeDHTLookupWaiting.WithLabelValues(stage, reason).Set(float64(waiting))
	if !observedAt.IsZero() {
		m.edgeDHTLookupLast.WithLabelValues(stage, reason).Set(timestampSeconds(observedAt))
	}
}

func validOutcome(outcome string) bool {
	return outcome == OutcomeOK || outcome == OutcomeError
}

func validFollowSyncOutcome(outcome string) bool {
	switch outcome {
	case FollowSyncCompleted, FollowSyncNoop, FollowSyncSuperseded, OutcomeError:
		return true
	default:
		return false
	}
}

func validEdgePublicationOutcome(outcome string) bool {
	switch outcome {
	case OutcomeOK, OutcomeError, EdgePublicationOutcomeTimeout, EdgePublicationOutcomeCanceled:
		return true
	default:
		return false
	}
}

func validEdgePublicationOperation(operation string) bool {
	return operation == EdgePublicationOperationPublish || operation == EdgePublicationOperationRestore
}

func validEdgePublicationStage(stage string) bool {
	switch stage {
	case EdgePublicationStageAdmission,
		EdgePublicationStageValidation,
		EdgePublicationStageProvideDocument,
		EdgePublicationStagePersistState,
		EdgePublicationStagePutRecord,
		EdgePublicationStageComplete:
		return true
	default:
		return false
	}
}

func validEdgeDHTQueryEvent(event string) bool {
	switch event {
	case EdgeDHTQueryEventSendingQuery,
		EdgeDHTQueryEventPeerResponse,
		EdgeDHTQueryEventQueryError,
		EdgeDHTQueryEventDialingPeer,
		EdgeDHTQueryEventValueRPC:
		return true
	default:
		return false
	}
}

func validEdgeDHTLookupTermination(reason string) bool {
	switch reason {
	case EdgeDHTLookupTerminationStopped,
		EdgeDHTLookupTerminationCancelled,
		EdgeDHTLookupTerminationStarvation,
		EdgeDHTLookupTerminationCompleted:
		return true
	default:
		return false
	}
}

func validIPNSPublicationStage(stage string) bool {
	return stage == IPNSStageProvideDocument || stage == IPNSStagePutRecord
}

func validRendezvousOperation(operation string) bool {
	return operation == RendezvousOperationProvide || operation == RendezvousOperationDiscover
}

func validRendezvousDiscoveryOutcome(outcome string) bool {
	switch outcome {
	case RendezvousDiscoveryAvailable, RendezvousDiscoveryEmpty, RendezvousDiscoveryTimeout:
		return true
	default:
		return false
	}
}

func validPointerKind(kind string) bool {
	switch kind {
	case PointerKindRoot, PointerKindManifest, PointerKindDocument:
		return true
	default:
		return false
	}
}

func validPointerRetryReason(reason string) bool {
	switch reason {
	case PointerRetryIneligible, PointerRetryCheckError, PointerRetryProvideError:
		return true
	default:
		return false
	}
}

func timestampSeconds(at time.Time) float64 {
	return float64(at.Unix()) + float64(at.Nanosecond())/float64(time.Second)
}

// FollowPoll records one channel's answer to one poll (spec 11.3). channel is
// ChannelHTTPS or ChannelIPNS; outcome is OutcomeOK or OutcomeError.
//
// It is recorded where the follower decides what a channel gave it, rather than
// at the HTTP client, which is what lets it say the two things an operator needs
// and a transport-level counter cannot: that an IPNS-only follower is polling at
// all, and that a document which arrived with a 200 and then failed its
// signature check is a failure.
func (m *Metrics) FollowPoll(channel, outcome string) {
	if m == nil {
		return
	}
	m.followPolls.WithLabelValues(channel, outcome).Inc()
}

// FollowAdmission records one complete resolve-and-admit phase. It deliberately
// excludes retained-closure sync: the two phases run independently in the
// daemon, and an operator needs to distinguish publication freshness from
// background replication cost.
func (m *Metrics) FollowAdmission(outcome string, duration time.Duration, completedAt time.Time) {
	if m == nil || !validOutcome(outcome) || duration < 0 {
		return
	}
	m.followAdmissionDuration.WithLabelValues(outcome).Observe(duration.Seconds())
	if outcome == OutcomeOK && !completedAt.IsZero() {
		m.followAdmissionLastOK.Set(timestampSeconds(completedAt))
	}
}

// FollowSync records one configured head's retained-closure pass. Only an
// actual CAS-stamped completion advances last-success: a nil-returning no-op or
// a pass superseded by a newer adoption is useful control flow, not evidence
// that the current closure is complete.
func (m *Metrics) FollowSync(head, outcome string, duration time.Duration, completedAt time.Time) {
	if m == nil || head == "" || !validFollowSyncOutcome(outcome) || duration < 0 {
		return
	}
	m.followSyncDuration.WithLabelValues(head, outcome).Observe(duration.Seconds())
	if outcome == FollowSyncCompleted && !completedAt.IsZero() {
		m.followSyncLastOK.WithLabelValues(head).Set(timestampSeconds(completedAt))
	}
}

// FollowSyncActive reports whether the single retained-closure sync permit is
// held. Direct Poll callers share that permit with the daemon worker.
func (m *Metrics) FollowSyncActive(active bool) {
	if m == nil {
		return
	}
	value := 0.0
	if active {
		value = 1
	}
	m.followSyncActive.Set(value)
}

// FollowSyncCoalesced counts a wakeup folded into an already-pending dirty bit.
func (m *Metrics) FollowSyncCoalesced() {
	if m == nil {
		return
	}
	m.followSyncCoalesced.Inc()
}

// FollowRefusal counts one adoption refusal (spec 11.3, 11.4). reason is one of the
// Refusal constants, which act at the per-head, document, or channel level (see them).
//
// It is separate from FollowPoll because the two count different things: a poll is a
// document, judged at resolution, and a refusal is a defence judged later, under the
// transition lock, when the resolved document is admitted. A document that resolves
// cleanly and is then refused is one FollowPoll(ok) and one FollowRefusal -- which is
// exactly the split a per-poll counter alone hid.
func (m *Metrics) FollowRefusal(reason string) {
	if m == nil {
		return
	}
	m.followRefusals.WithLabelValues(reason).Inc()
}

// FollowSyncedToFloorLag publishes the width of head's synced_to-floor
// divergence window (spec 11.3): the slots the follower still serves as covered
// while the writer has retracted below them. lag is the no-regression floor
// minus a publication's synced_to at the moment that publication is refused on
// the floor, and 0 when a publication is accepted.
//
// It is a gauge and not a counter because the window is a state, not an event:
// what an operator watches is that it is nonzero, and for how long. Unlike
// FollowRefusal, which ticks once per refused poll, this holds the current lag
// between polls -- the window is open only while the writer's published coverage
// stays below the floor, and closes when it climbs back past it.
func (m *Metrics) FollowSyncedToFloorLag(head string, lag uint64) {
	if m == nil {
		return
	}
	m.followFloorLag.WithLabelValues(head).Set(float64(lag))
}

// FollowHeadReady records whether a configured followed head is currently served
// . It is set 0 for every configured followed head at startup, so a
// head that never registers reads 0 rather than being absent from the metric, and
// set 1 the first time the follower resumes or adopts it. An ordinary poll failure
// does not lower it -- a head that has served once stays up on its durable root
// while the writer is unreachable -- but a quarantine does (spec 11.4): a head that
// served data which does not verify is taken out of service, and returns to 0.
func (m *Metrics) FollowHeadReady(head string, ready bool) {
	if m == nil {
		return
	}
	v := 0.0
	if ready {
		v = 1
	}
	m.followReady.WithLabelValues(head).Set(v)
}

// ConfigureFollowSourceMetrics declares the exact locally authorized
// source/head cells which source-set rollout telemetry may use. It materializes
// availability, freshness, covered, and selected baselines at zero; synced_to
// remains absent until a covered claim has actually been observed. A later call
// replaces the set without resetting unchanged cells and deletes every series
// belonging only to a retired source or authorization cell.
func (m *Metrics) ConfigureFollowSourceMetrics(headSources map[string][]string) {
	if m == nil {
		return
	}
	sources := make(map[string]struct{})
	cells := make(map[followSourceHeadCell]struct{})
	for head, configured := range headSources {
		if head == "" {
			continue
		}
		for _, source := range configured {
			if source == "" {
				continue
			}
			sources[source] = struct{}{}
			cells[followSourceHeadCell{head: head, source: source}] = struct{}{}
		}
	}

	m.followSourceLabelsMu.Lock()
	defer m.followSourceLabelsMu.Unlock()
	for source := range m.followSources {
		if _, keep := sources[source]; keep {
			continue
		}
		m.followSourceAvailable.DeleteLabelValues(source)
		m.followSourceLastSuccess.DeleteLabelValues(source)
	}
	for cell := range m.followSourceCells {
		if _, keep := cells[cell]; keep {
			continue
		}
		m.followSourceCovered.DeleteLabelValues(cell.head, cell.source)
		m.followSourceSyncedTo.DeleteLabelValues(cell.head, cell.source)
		m.followSourceSelected.DeleteLabelValues(cell.head, cell.source)
	}
	for source := range sources {
		if _, exists := m.followSources[source]; exists {
			continue
		}
		m.followSourceAvailable.WithLabelValues(source).Set(0)
		m.followSourceLastSuccess.WithLabelValues(source).Set(0)
	}
	for cell := range cells {
		if _, exists := m.followSourceCells[cell]; exists {
			continue
		}
		m.followSourceCovered.WithLabelValues(cell.head, cell.source).Set(0)
		m.followSourceSelected.WithLabelValues(cell.head, cell.source).Set(0)
	}
	m.followSources = sources
	m.followSourceCells = cells
}

// FollowSourceAvailable records whether one configured source produced a
// usable authenticated publication in the latest serialized source-set poll.
// A success also advances its freshness timestamp. Unknown labels are ignored.
func (m *Metrics) FollowSourceAvailable(source string, available bool) {
	if m == nil {
		return
	}
	m.followSourceLabelsMu.RLock()
	defer m.followSourceLabelsMu.RUnlock()
	if _, ok := m.followSources[source]; !ok {
		return
	}
	m.followSourceAvailable.WithLabelValues(source).Set(boolMetric(available))
	if available {
		m.followSourceLastSuccess.WithLabelValues(source).SetToCurrentTime()
	}
}

// FollowSourceHeadClaim records the latest successfully authenticated claim
// for one configured source/head cell. An uncovered claim removes synced_to so
// slot zero cannot be confused with absence. An outage does not call this
// method, deliberately retaining the last observation beside available=0.
func (m *Metrics) FollowSourceHeadClaim(head, source string, syncedTo uint64, covered bool) {
	if m == nil {
		return
	}
	m.followSourceLabelsMu.RLock()
	defer m.followSourceLabelsMu.RUnlock()
	cell := followSourceHeadCell{head: head, source: source}
	if _, ok := m.followSourceCells[cell]; !ok {
		return
	}
	m.followSourceCovered.WithLabelValues(head, source).Set(boolMetric(covered))
	if !covered {
		m.followSourceSyncedTo.DeleteLabelValues(head, source)
		return
	}
	m.followSourceSyncedTo.WithLabelValues(head, source).Set(float64(syncedTo))
}

// FollowSourceSelected publishes source provenance for head's durable last-good
// selected checkpoint. The gauge is one-hot across locally configured cells;
// readiness separately reports whether that checkpoint is currently served.
// An empty source clears the selection; an unknown nonempty source is ignored
// so a corrupted checkpoint cannot create a label.
func (m *Metrics) FollowSourceSelected(head, source string) {
	if m == nil {
		return
	}
	m.followSourceLabelsMu.RLock()
	defer m.followSourceLabelsMu.RUnlock()
	if source != "" {
		if _, ok := m.followSourceCells[followSourceHeadCell{head: head, source: source}]; !ok {
			return
		}
	}
	for cell := range m.followSourceCells {
		if cell.head != head {
			continue
		}
		m.followSourceSelected.WithLabelValues(cell.head, cell.source).Set(boolMetric(source != "" && cell.source == source))
	}
}

// ConfigureFollowConflictMetrics declares the exact locally configured
// head/source label cells that multi-writer conflict telemetry may use. It
// materializes every declared series at zero so dashboards and alerts have a
// baseline before the first event. A later call replaces the configured set:
// unchanged cells retain their values, removed cells are deleted, and new cells
// start at zero. Source-set reconfiguration therefore cannot leave a retired
// source or head exporting stale state. Re-adding a previously removed counter
// cell starts it at zero, as Prometheus process-local counters normally do after
// a configuration lifecycle ends.
//
// A head is registered even when its source list is empty, while an empty head
// or source name is ignored. Runtime observation methods refuse every label
// outside this registry. The source-counter cells can therefore be only the
// caller's bounded authorization relation, rather than the full head/source
// cross product or an untrusted value copied from a publication document.
func (m *Metrics) ConfigureFollowConflictMetrics(headSources map[string][]string) {
	if m == nil {
		return
	}

	heads := make(map[string]struct{}, len(headSources))
	sourceCells := make(map[followConflictSourceCell]struct{})
	for head, sources := range headSources {
		if head == "" {
			continue
		}
		heads[head] = struct{}{}
		for _, source := range sources {
			if source != "" {
				sourceCells[followConflictSourceCell{head: head, source: source}] = struct{}{}
			}
		}
	}

	m.followConflictLabelsMu.Lock()
	defer m.followConflictLabelsMu.Unlock()
	for head := range m.followConflictHeads {
		if _, keep := heads[head]; keep {
			continue
		}
		m.followConflictActive.DeleteLabelValues(head)
		m.followIncomparableActive.DeleteLabelValues(head)
		m.followIncomparables.DeleteLabelValues(head)
	}
	for cell := range m.followConflictSourceCells {
		if _, keep := sourceCells[cell]; !keep {
			m.followConflicts.DeleteLabelValues(cell.head, cell.source)
		}
	}
	for head := range heads {
		if _, ok := m.followConflictHeads[head]; !ok {
			m.followConflictActive.WithLabelValues(head).Set(0)
			m.followIncomparableActive.WithLabelValues(head).Set(0)
			m.followIncomparables.WithLabelValues(head).Add(0)
		}
	}
	for cell := range sourceCells {
		if _, ok := m.followConflictSourceCells[cell]; !ok {
			m.followConflicts.WithLabelValues(cell.head, cell.source).Add(0)
		}
	}
	m.followConflictHeads = heads
	m.followConflictSourceCells = sourceCells
}

// FollowConflictActive publishes whether head has a durable hard-conflict
// latch. Unknown heads are ignored so evidence cannot create a label.
func (m *Metrics) FollowConflictActive(head string, active bool) {
	if m == nil {
		return
	}
	m.followConflictLabelsMu.RLock()
	defer m.followConflictLabelsMu.RUnlock()
	if _, ok := m.followConflictHeads[head]; !ok {
		return
	}
	m.followConflictActive.WithLabelValues(head).Set(boolMetric(active))
}

// FollowConflictCreated counts one newly persisted hard-conflict latch for a
// configured head/source evidence cell. A conflict involving two sources calls
// this once for each source; an already-active latch must not call it again.
func (m *Metrics) FollowConflictCreated(head, source string) {
	if m == nil {
		return
	}
	m.followConflictLabelsMu.RLock()
	defer m.followConflictLabelsMu.RUnlock()
	if _, ok := m.followConflictSourceCells[followConflictSourceCell{head: head, source: source}]; !ok {
		return
	}
	m.followConflicts.WithLabelValues(head, source).Inc()
}

// FollowIncomparableActive publishes whether head's latest arbitration result
// is transiently incomparable. It is deliberately separate from the durable
// conflict latch: convergence may clear this gauge without operator action.
func (m *Metrics) FollowIncomparableActive(head string, active bool) {
	if m == nil {
		return
	}
	m.followConflictLabelsMu.RLock()
	defer m.followConflictLabelsMu.RUnlock()
	if _, ok := m.followConflictHeads[head]; !ok {
		return
	}
	m.followIncomparableActive.WithLabelValues(head).Set(boolMetric(active))
}

// FollowIncomparableObserved counts one transient incomparable arbitration
// result. Unknown heads are ignored.
func (m *Metrics) FollowIncomparableObserved(head string) {
	if m == nil {
		return
	}
	m.followConflictLabelsMu.RLock()
	defer m.followConflictLabelsMu.RUnlock()
	if _, ok := m.followConflictHeads[head]; !ok {
		return
	}
	m.followIncomparables.WithLabelValues(head).Inc()
}

func boolMetric(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
