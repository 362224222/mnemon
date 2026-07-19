package artifact

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	gcDefaultPeriod       = time.Minute
	gcDefaultObjectTTL    = time.Hour
	gcDefaultStagingTTL   = time.Hour
	gcDefaultTempTTL      = time.Hour
	gcDefaultMaxExamined  = 128
	gcDefaultMaxQueued    = 32
	gcDefaultMaxBytes     = 16 << 20
	gcDefaultMaxTemps     = 32
	gcDefaultMaxQueue     = 128
	gcStagingCycleBytes   = MaxTotalBytes
	gcMaximumPeriod       = time.Hour
	gcMaximumTTL          = 365 * 24 * time.Hour
	gcMaximumBytes        = MaxTotalBytes
	gcMaximumTemps        = 256
	gcMaximumQueue        = 256
	gcMaximumStagingPass  = uint64(1<<63 - 1)
	gcRandomTokenAttempts = 4
)

var (
	ErrGC               = errors.New("Artifact GC")
	ErrGCRunning        = fmt.Errorf("%w: worker has already run", ErrGC)
	ErrGCInvariant      = fmt.Errorf("%w: invariant failed", ErrGC)
	ErrGCStoreInvariant = errors.New("Artifact GC Store invariant")
)

// GCScanCursor is the Store-owned restart point for one frozen object scan.
// Done closes a pass; OpenArtifactGCScan must initialize a fresh pass before
// returning it to a worker again.
type GCScanCursor struct {
	Cutoff time.Time
	After  model.Digest
	Done   bool
}

type GCScanSpec struct {
	InitializeCutoff time.Time
	At               time.Time
}

type GCCandidate struct {
	Digest     model.Digest
	SizeBytes  uint64
	ModifiedAt time.Time
	Token      [32]byte
}

// GCPrepareSpec is one atomic Store decision. The Store examines candidates
// in order, decides protection from durable authority, enqueues each selected
// candidate with its exact token, and advances Current past every examined
// candidate. PageDone may be persisted only after every candidate was
// examined. MaxQueue is the total durable queue cap, not a hint.
type GCPrepareSpec struct {
	Current    GCScanCursor
	Candidates []GCCandidate
	PageDone   bool
	MaxQueued  int
	MaxQueue   int
	At         time.Time
}

type GCPrepareResult struct {
	Next        GCScanCursor
	Examined    int
	Protected   int
	Queued      int
	QueuedBytes uint64
}

// GCStagingScanCursor is the Store-owned restart point for one frozen staging
// metadata scan. Generation distinguishes two passes that begin with the same
// cutoff while a trusted clock is constant; without it, an exact response-loss
// receipt for the prior pass could be mistaken for the first page of the next
// pass.
type GCStagingScanCursor struct {
	Generation uint64
	Cutoff     time.Time
	After      model.Digest
	Done       bool
}

type GCStagingSweepSpec struct {
	Current  GCStagingScanCursor
	MaxItems int
	MaxBytes uint64
	At       time.Time
}

// GCStagingSweepResult deliberately contains counts only. Classification of
// failed, expired, accepted, or otherwise protected staging belongs entirely
// to the Store transaction.
type GCStagingSweepResult struct {
	Next       GCStagingScanCursor
	Examined   int
	Swept      int
	SweptBytes uint64
}

type GCQueueState string

const (
	GCQueueQueued  GCQueueState = "queued"
	GCQueueRenamed GCQueueState = "renamed"
)

type GCQueueIdentity struct {
	Digest model.Digest
	Token  [32]byte
}

type GCQueueItem struct {
	Identity   GCQueueIdentity
	SizeBytes  uint64
	ModifiedAt time.Time
	QueuedAt   time.Time
	RenamedAt  time.Time
	State      GCQueueState
}

type GCQueueTransitionSpec struct {
	Identity GCQueueIdentity
	At       time.Time
}

// GCQueueTransitionResult is shared by MarkArtifactGCRenamed and
// CompleteArtifactGC. Mark returns State=renamed, Completed=false; Complete
// returns an empty State and Completed=true. Replayed reports a durable
// same-identity replay, never an inferred filesystem outcome.
type GCQueueTransitionResult struct {
	State     GCQueueState
	Completed bool
	Replayed  bool
	At        time.Time
}

// GCStore is the complete durable authority used by the collector. The
// artifact package intentionally does not import the concrete Store package.
// Implementations must make PrepareArtifactGC's protection test, enqueue, and
// cursor advance one transaction.
type GCStore interface {
	OpenArtifactGCScan(context.Context, GCScanSpec) (GCScanCursor, error)
	PrepareArtifactGC(context.Context, GCPrepareSpec) (GCPrepareResult, error)
	OpenArtifactGCStagingScan(context.Context, GCScanSpec) (GCStagingScanCursor, error)
	SweepArtifactGCStaging(context.Context, GCStagingSweepSpec) (GCStagingSweepResult, error)
	ListArtifactGCQueue(context.Context, int) ([]GCQueueItem, error)
	GetArtifactGCQueue(context.Context, GCQueueIdentity) (GCQueueItem, bool, error)
	MarkArtifactGCRenamed(context.Context, GCQueueTransitionSpec) (GCQueueTransitionResult, error)
	CompleteArtifactGC(context.Context, GCQueueTransitionSpec) (GCQueueTransitionResult, error)
}

type GCOptions struct {
	Store  GCStore
	CAS    *CAS
	Clock  Clock
	Random io.Reader

	Period     time.Duration
	ObjectTTL  time.Duration
	StagingTTL time.Duration
	TempTTL    time.Duration

	MaxExamined int
	MaxQueued   int
	// MaxBytes bounds physical CAS queue/tombstone bytes per cycle. Staging
	// metadata has a separate fixed MaxTotalBytes logical-root bound.
	MaxBytes uint64
	MaxTemps int
	MaxQueue int
}

type GCState string

const (
	GCIdle    GCState = "idle"
	GCRunning GCState = "running"
	GCStopped GCState = "stopped"
	GCFailed  GCState = "failed"
)

type GCFatalCode string

const (
	GCFatalNone            GCFatalCode = ""
	GCFatalStoreInvariant  GCFatalCode = "store_invariant"
	GCFatalStoreFailure    GCFatalCode = "store_failure"
	GCFatalCASInvariant    GCFatalCode = "cas_invariant"
	GCFatalCASFailure      GCFatalCode = "cas_failure"
	GCFatalWorkerInvariant GCFatalCode = "worker_invariant"
)

// GCSnapshot is intentionally a closed operational projection: it contains
// only state, a closed fatal code, and counters. It never carries an object
// digest, token, path, Store diagnostic, or error text.
type GCSnapshot struct {
	State                  GCState
	FatalCode              GCFatalCode
	Cycles                 uint64
	StartupReconciliations uint64
	Reconciliations        uint64
	QueueItemsCompleted    uint64
	ObjectsTombstoned      uint64
	TombstonesPurged       uint64
	TempsPruned            uint64
	StagingExamined        uint64
	StagingSwept           uint64
	StagingBytesSwept      uint64
	ObjectsExamined        uint64
	ObjectsProtected       uint64
	ObjectsQueued          uint64
	ObjectBytesQueued      uint64
}

type GCWorker struct {
	store  GCStore
	cas    *CAS
	clock  Clock
	random io.Reader

	period     time.Duration
	objectTTL  time.Duration
	stagingTTL time.Duration
	tempTTL    time.Duration

	maxExamined int
	maxQueued   int
	maxBytes    uint64
	maxTemps    int
	maxQueue    int

	cycle sync.Mutex
	mu    sync.Mutex

	started  bool
	running  bool
	snapshot GCSnapshot
}

func NewGCWorker(options GCOptions) (*GCWorker, error) {
	if options.Store == nil || options.CAS == nil {
		return nil, fmt.Errorf("%w: Store and CAS are required", ErrGC)
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	applyGCDefaults(&options)
	if err := validateGCOptions(options); err != nil {
		return nil, err
	}
	return &GCWorker{
		store: options.Store, cas: options.CAS, clock: options.Clock, random: options.Random,
		period: options.Period, objectTTL: options.ObjectTTL, stagingTTL: options.StagingTTL,
		tempTTL: options.TempTTL, maxExamined: options.MaxExamined, maxQueued: options.MaxQueued,
		maxBytes: options.MaxBytes, maxTemps: options.MaxTemps, maxQueue: options.MaxQueue,
		snapshot: GCSnapshot{State: GCIdle},
	}, nil
}

func applyGCDefaults(options *GCOptions) {
	if options.Period == 0 {
		options.Period = gcDefaultPeriod
	}
	if options.ObjectTTL == 0 {
		options.ObjectTTL = gcDefaultObjectTTL
	}
	if options.StagingTTL == 0 {
		options.StagingTTL = gcDefaultStagingTTL
	}
	if options.TempTTL == 0 {
		options.TempTTL = gcDefaultTempTTL
	}
	if options.MaxExamined == 0 {
		options.MaxExamined = gcDefaultMaxExamined
	}
	if options.MaxQueued == 0 {
		options.MaxQueued = gcDefaultMaxQueued
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = gcDefaultMaxBytes
	}
	if options.MaxTemps == 0 {
		options.MaxTemps = gcDefaultMaxTemps
	}
	if options.MaxQueue == 0 {
		options.MaxQueue = gcDefaultMaxQueue
	}
}

func validateGCOptions(options GCOptions) error {
	if options.Period <= 0 || options.Period > gcMaximumPeriod ||
		options.ObjectTTL <= 0 || options.ObjectTTL > gcMaximumTTL ||
		options.StagingTTL <= 0 || options.StagingTTL > gcMaximumTTL ||
		options.TempTTL <= 0 || options.TempTTL > gcMaximumTTL ||
		options.MaxExamined <= 0 || options.MaxExamined > maxCASObjectPageSize ||
		options.MaxQueued <= 0 || options.MaxQueued > options.MaxExamined ||
		options.MaxBytes < maxCASObjectSize || options.MaxBytes > gcMaximumBytes ||
		options.MaxTemps <= 0 || options.MaxTemps > gcMaximumTemps ||
		options.MaxQueue <= 0 || options.MaxQueue > gcMaximumQueue ||
		options.MaxQueued > options.MaxQueue {
		return fmt.Errorf("%w: invalid bounded worker configuration", ErrGC)
	}
	return nil
}

func (worker *GCWorker) Snapshot() GCSnapshot {
	if worker == nil {
		return GCSnapshot{State: GCFailed, FatalCode: GCFatalWorkerInvariant}
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.snapshot
}

// ReconcileStartup synchronously closes every durable queue/tombstone crash
// state before capture, import, receiver, or serving workers start using CAS.
// It is idempotent and may be called again after an unclean shutdown.
func (worker *GCWorker) ReconcileStartup(ctx context.Context) error {
	if err := worker.usable(ctx); err != nil {
		return err
	}
	worker.cycle.Lock()
	defer worker.cycle.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	lease, err := worker.cas.AcquireExclusive()
	if err != nil {
		return worker.recordFailure(worker.casFailure("acquire startup lifecycle fence", err))
	}
	defer lease.Release()
	if err := ctx.Err(); err != nil {
		return err
	}
	at, err := canonicalGCTime(worker.clock())
	if err != nil {
		return worker.recordFailure(gcFatal(GCFatalWorkerInvariant, "read startup trusted clock", err))
	}
	for {
		result, reconcileErr := worker.reconcileBatch(ctx, at, worker.maxQueued, worker.maxBytes)
		if reconcileErr != nil {
			if gcContextCancellation(reconcileErr, ctx) {
				return ctx.Err()
			}
			return worker.recordFailure(reconcileErr)
		}
		if !result.Remaining {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	worker.mu.Lock()
	worker.snapshot.StartupReconciliations++
	worker.mu.Unlock()
	return nil
}

// Run executes one immediate cycle and then periodic bounded cycles. A worker
// value is single-use so two schedulers cannot silently share its counters or
// durable scan progression.
func (worker *GCWorker) Run(ctx context.Context) error {
	if err := worker.usable(ctx); err != nil {
		return err
	}
	worker.mu.Lock()
	if worker.started || worker.running {
		worker.mu.Unlock()
		return ErrGCRunning
	}
	worker.started = true
	worker.running = true
	worker.snapshot.State = GCRunning
	worker.snapshot.FatalCode = GCFatalNone
	worker.mu.Unlock()

	failed := false
	defer func() {
		worker.mu.Lock()
		worker.running = false
		if !failed {
			worker.snapshot.State = GCStopped
		}
		worker.mu.Unlock()
	}()
	ticker := time.NewTicker(worker.period)
	defer ticker.Stop()
	for {
		if err := worker.runCycle(ctx); err != nil {
			if gcContextCancellation(err, ctx) {
				return nil
			}
			failed = true
			worker.recordFailure(err)
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// RunCycle is the synchronous bounded cycle used by daemon startup tests and
// deterministic process recovery. Cancellation can stop only between durable
// Store/CAS boundaries; an in-progress filesystem transition is completed to
// its next replayable state first.
func (worker *GCWorker) RunCycle(ctx context.Context) error {
	if err := worker.usable(ctx); err != nil {
		return err
	}
	if err := worker.runCycle(ctx); err != nil {
		if gcContextCancellation(err, ctx) {
			return ctx.Err()
		}
		return worker.recordFailure(err)
	}
	return nil
}

func (worker *GCWorker) runCycle(ctx context.Context) error {
	worker.cycle.Lock()
	defer worker.cycle.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	lease, err := worker.cas.AcquireExclusive()
	if err != nil {
		return worker.casFailure("acquire collection lifecycle fence", err)
	}
	defer lease.Release()
	if err := ctx.Err(); err != nil {
		return err
	}
	now, err := canonicalGCTime(worker.clock())
	if err != nil {
		return gcFatal(GCFatalWorkerInvariant, "read trusted clock", err)
	}
	objectCutoff, err := canonicalGCTime(now.Add(-worker.objectTTL))
	if err != nil {
		return gcFatal(GCFatalWorkerInvariant, "derive object cutoff", err)
	}
	stagingCutoff, err := canonicalGCTime(now.Add(-worker.stagingTTL))
	if err != nil {
		return gcFatal(GCFatalWorkerInvariant, "derive staging cutoff", err)
	}
	tempCutoff, err := canonicalGCTime(now.Add(-worker.tempTTL))
	if err != nil {
		return gcFatal(GCFatalWorkerInvariant, "derive temp cutoff", err)
	}
	worker.mu.Lock()
	worker.snapshot.Cycles++
	worker.mu.Unlock()
	initial, err := worker.reconcileBatch(ctx, now, worker.maxQueued, worker.maxBytes)
	if err != nil {
		return err
	}
	// A periodic cycle never creates more work while startup/restart work still
	// exceeds this cycle's item or byte budget. The next tick resumes from the
	// same durable queue without weakening the exclusive boundary.
	if initial.Remaining {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	removed, err := worker.cas.PruneTempsBefore(tempCutoff, worker.maxTemps)
	if err != nil {
		return worker.casFailure("prune recognizable temps", err)
	}
	worker.mu.Lock()
	worker.snapshot.TempsPruned += uint64(len(removed))
	worker.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Queue recovery, staging examination, and new object preparation share one
	// cycle-wide item budget. Physical queue/tombstone work separately shares
	// MaxBytes between old and new objects. An atomic staging-root sweep has a
	// fixed MaxTotalBytes logical budget because one root may be larger than one
	// physical CAS object. Temp names and durable backlog use MaxTemps/MaxQueue.
	remainingExamined := worker.maxExamined - initial.Processed
	if remainingExamined == 0 {
		return nil
	}
	stagingCursor, err := worker.store.OpenArtifactGCStagingScan(ctx,
		GCScanSpec{InitializeCutoff: stagingCutoff, At: now})
	if err != nil {
		if gcContextCancellation(err, ctx) {
			return ctx.Err()
		}
		return worker.storeFailure("open durable staging scan", err)
	}
	if err := validateGCStagingScanCursor(stagingCursor, stagingCutoff, false); err != nil {
		return gcFatal(GCFatalStoreInvariant, "validate durable staging scan", err)
	}
	swept, err := worker.store.SweepArtifactGCStaging(ctx, GCStagingSweepSpec{
		Current: stagingCursor, MaxItems: remainingExamined, MaxBytes: gcStagingCycleBytes, At: now,
	})
	if err != nil {
		if gcContextCancellation(err, ctx) {
			return ctx.Err()
		}
		return worker.storeFailure("sweep unaccepted staging", err)
	}
	if err := validateGCStagingSweep(swept, stagingCursor, remainingExamined,
		gcStagingCycleBytes); err != nil {
		return gcFatal(GCFatalStoreInvariant, "validate staging sweep result", err)
	}
	worker.mu.Lock()
	worker.snapshot.StagingExamined += uint64(swept.Examined)
	worker.snapshot.StagingSwept += uint64(swept.Swept)
	worker.snapshot.StagingBytesSwept += swept.SweptBytes
	worker.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	remainingExamined -= swept.Examined
	remainingBytes := worker.maxBytes - initial.ProcessedBytes
	remainingQueued := worker.maxQueued - initial.Processed
	if remainingExamined == 0 || remainingQueued == 0 || remainingBytes == 0 {
		return nil
	}
	if err := worker.prepare(ctx, objectCutoff, now, remainingExamined,
		remainingQueued, remainingBytes); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	settled, err := worker.reconcileBatch(ctx, now, remainingQueued, remainingBytes)
	if err != nil {
		return err
	}
	if settled.Remaining {
		return gcFatal(GCFatalStoreInvariant,
			"new collection queue exceeded its preparation budget", nil)
	}
	return nil
}

func (worker *GCWorker) prepare(ctx context.Context, initializeCutoff, at time.Time,
	maxExamined, maxQueued int, maxBytes uint64,
) error {
	cursor, err := worker.store.OpenArtifactGCScan(ctx,
		GCScanSpec{InitializeCutoff: initializeCutoff, At: at})
	if err != nil {
		if gcContextCancellation(err, ctx) {
			return ctx.Err()
		}
		return worker.storeFailure("open durable object scan", err)
	}
	if err := validateGCScanCursor(cursor, initializeCutoff, false); err != nil {
		return gcFatal(GCFatalStoreInvariant, "validate durable object scan", err)
	}
	casCursor, err := NewCASObjectScanCursor(cursor.Cutoff, cursor.After)
	if err != nil {
		return worker.casFailure("construct CAS object scan", err)
	}
	page, err := worker.cas.ListObjectsPage(casCursor, maxExamined)
	if err != nil {
		return worker.casFailure("scan CAS objects", err)
	}
	if err := validateGCObjectPage(page, cursor, maxExamined); err != nil {
		return gcFatal(GCFatalCASInvariant, "validate CAS object page", err)
	}
	candidates, pageDone, err := worker.boundCandidates(page, maxBytes)
	if err != nil {
		return err
	}
	if len(candidates) == 0 && !pageDone {
		// The remainder of this cycle's byte budget cannot fit the next
		// canonical object. A fresh cycle gets the full budget and resumes from
		// the unchanged durable cursor.
		return nil
	}
	prepared := make([]GCCandidate, len(candidates))
	for index, candidate := range candidates {
		token, tokenErr := worker.newToken()
		if tokenErr != nil {
			return gcFatal(GCFatalWorkerInvariant, "generate collection token", tokenErr)
		}
		prepared[index] = GCCandidate{Digest: candidate.Digest, SizeBytes: candidate.Size,
			ModifiedAt: candidate.ModifiedAt, Token: token}
	}
	result, err := worker.store.PrepareArtifactGC(ctx, GCPrepareSpec{
		Current: cursor, Candidates: prepared, PageDone: pageDone,
		MaxQueued: maxQueued, MaxQueue: worker.maxQueue, At: at,
	})
	if err != nil {
		if gcContextCancellation(err, ctx) {
			return ctx.Err()
		}
		return worker.storeFailure("prepare durable collection queue", err)
	}
	if err := validateGCPrepareResult(result, cursor, prepared, pageDone,
		maxQueued, maxBytes); err != nil {
		return gcFatal(GCFatalStoreInvariant, "validate collection preparation", err)
	}
	if err := worker.validatePreparedQueue(ctx, prepared[:result.Examined], result, at); err != nil {
		return err
	}
	worker.mu.Lock()
	worker.snapshot.ObjectsExamined += uint64(result.Examined)
	worker.snapshot.ObjectsProtected += uint64(result.Protected)
	worker.snapshot.ObjectsQueued += uint64(result.Queued)
	worker.snapshot.ObjectBytesQueued += result.QueuedBytes
	worker.mu.Unlock()
	return nil
}

func (worker *GCWorker) validatePreparedQueue(ctx context.Context, examined []GCCandidate,
	result GCPrepareResult, at time.Time,
) error {
	queue, err := worker.store.ListArtifactGCQueue(ctx, worker.maxQueue+1)
	if err != nil {
		if gcContextCancellation(err, ctx) {
			return ctx.Err()
		}
		return worker.storeFailure("verify prepared collection queue", err)
	}
	if len(queue) != result.Queued || len(queue) > worker.maxQueue {
		return gcFatal(GCFatalStoreInvariant,
			"prepared queue count differs from durable result", nil)
	}
	if err := validateGCQueue(queue, at); err != nil {
		return gcFatal(GCFatalStoreInvariant, "validate prepared durable queue", err)
	}
	expected := make(map[model.Digest]GCCandidate, len(examined))
	for _, candidate := range examined {
		expected[candidate.Digest] = candidate
	}
	var queuedBytes uint64
	for _, item := range queue {
		candidate, present := expected[item.Identity.Digest]
		if !present || item.Identity.Token != candidate.Token ||
			item.SizeBytes != candidate.SizeBytes || item.ModifiedAt != candidate.ModifiedAt ||
			item.QueuedAt != at || item.State != GCQueueQueued || !item.RenamedAt.IsZero() {
			return gcFatal(GCFatalStoreInvariant,
				"prepared queue differs from exact candidate token or metadata", nil)
		}
		if item.SizeBytes > result.QueuedBytes ||
			queuedBytes > result.QueuedBytes-item.SizeBytes {
			return gcFatal(GCFatalStoreInvariant, "prepared queue byte count overflow", nil)
		}
		queuedBytes += item.SizeBytes
	}
	if queuedBytes != result.QueuedBytes {
		return gcFatal(GCFatalStoreInvariant,
			"prepared queue bytes differ from durable result", nil)
	}
	return nil
}

func (worker *GCWorker) boundCandidates(page CASObjectPage,
	maxBytes uint64,
) ([]CASObjectCandidate, bool, error) {
	var total uint64
	count := len(page.Candidates)
	for index, candidate := range page.Candidates {
		if candidate.Size > maxBytes || total > maxBytes-candidate.Size {
			count = index
			break
		}
		total += candidate.Size
	}
	selected := append([]CASObjectCandidate(nil), page.Candidates[:count]...)
	return selected, page.Done && count == len(page.Candidates), nil
}

type gcReconcileResult struct {
	Processed      int
	ProcessedBytes uint64
	Remaining      bool
}

func (worker *GCWorker) reconcileBatch(ctx context.Context, at time.Time,
	maxItems int, maxBytes uint64,
) (gcReconcileResult, error) {
	if err := ctx.Err(); err != nil {
		return gcReconcileResult{}, err
	}
	if maxItems <= 0 || maxItems > worker.maxQueued || maxBytes == 0 || maxBytes > worker.maxBytes {
		return gcReconcileResult{}, gcFatal(GCFatalWorkerInvariant,
			"invalid reconciliation batch budget", nil)
	}
	queue, err := worker.store.ListArtifactGCQueue(ctx, worker.maxQueue+1)
	if err != nil {
		if gcContextCancellation(err, ctx) {
			return gcReconcileResult{}, ctx.Err()
		}
		return gcReconcileResult{}, worker.storeFailure("list durable collection queue", err)
	}
	if len(queue) > worker.maxQueue {
		return gcReconcileResult{}, gcFatal(GCFatalStoreInvariant,
			"durable collection queue exceeds hard limit", nil)
	}
	if err := validateGCQueue(queue, at); err != nil {
		return gcReconcileResult{}, gcFatal(GCFatalStoreInvariant,
			"validate durable collection queue", err)
	}
	if err := ctx.Err(); err != nil {
		return gcReconcileResult{}, err
	}
	tombstones, err := worker.cas.ListTombstones(worker.maxQueue + 1)
	if err != nil {
		return gcReconcileResult{}, worker.casFailure("list durable CAS tombstones", err)
	}
	if len(tombstones) > worker.maxQueue {
		return gcReconcileResult{}, gcFatal(GCFatalCASInvariant,
			"CAS tombstones exceed durable queue limit", nil)
	}
	items := make(map[string]GCQueueItem, len(queue)+len(tombstones))
	for _, item := range queue {
		items[gcQueueKey(item.Identity)] = item
	}
	for _, descriptor := range tombstones {
		if err := validateGCTombstoneDescriptor(descriptor); err != nil {
			return gcReconcileResult{}, gcFatal(GCFatalCASInvariant,
				"validate CAS tombstone descriptor", err)
		}
		identity := GCQueueIdentity{Digest: descriptor.Digest, Token: descriptor.Token}
		item, found, getErr := worker.store.GetArtifactGCQueue(ctx, identity)
		if getErr != nil {
			if gcContextCancellation(getErr, ctx) {
				return gcReconcileResult{}, ctx.Err()
			}
			return gcReconcileResult{}, worker.storeFailure(
				"match CAS tombstone to durable queue", getErr)
		}
		if !found {
			return gcReconcileResult{}, gcFatal(GCFatalCASInvariant,
				"CAS tombstone has no durable queue owner", nil)
		}
		if err := validateGCQueueItem(item, at); err != nil || item.Identity != identity {
			return gcReconcileResult{}, gcFatal(GCFatalStoreInvariant,
				"validate tombstone queue owner", err)
		}
		items[gcQueueKey(identity)] = item
	}
	if len(items) > worker.maxQueue {
		return gcReconcileResult{}, gcFatal(GCFatalStoreInvariant,
			"durable collection ownership exceeds hard limit", nil)
	}
	ordered := make([]GCQueueItem, 0, len(items))
	seenDigest := make(map[model.Digest][32]byte, len(items))
	for _, item := range items {
		if token, present := seenDigest[item.Identity.Digest]; present && token != item.Identity.Token {
			return gcReconcileResult{}, gcFatal(GCFatalStoreInvariant,
				"digest has multiple durable collection tokens", nil)
		}
		seenDigest[item.Identity.Digest] = item.Identity.Token
		ordered = append(ordered, item)
	}
	sort.Slice(ordered, func(left, right int) bool {
		return gcQueueIdentityLess(ordered[left].Identity, ordered[right].Identity)
	})
	worker.mu.Lock()
	worker.snapshot.Reconciliations++
	worker.mu.Unlock()
	selected := 0
	var selectedBytes uint64
	for selected < len(ordered) && selected < maxItems {
		size := ordered[selected].SizeBytes
		if size > maxBytes-selectedBytes {
			break
		}
		selectedBytes += size
		selected++
	}
	if len(ordered) > 0 && selected == 0 {
		return gcReconcileResult{}, gcFatal(GCFatalStoreInvariant,
			"queued object cannot fit collection byte budget", nil)
	}
	for _, item := range ordered[:selected] {
		if err := worker.processQueueItem(ctx, item, at); err != nil {
			return gcReconcileResult{}, err
		}
	}
	return gcReconcileResult{Processed: selected, ProcessedBytes: selectedBytes,
		Remaining: selected < len(ordered)}, nil
}

func (worker *GCWorker) processQueueItem(ctx context.Context, item GCQueueItem, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	status, err := worker.cas.InspectTombstone(item.Identity.Digest, item.Identity.Token)
	if err != nil {
		return worker.casFailure("inspect queued CAS object", err)
	}
	if err := validateGCTombstoneStatus(status); err != nil {
		return gcFatal(GCFatalCASInvariant, "validate queued CAS state", err)
	}
	switch item.State {
	case GCQueueQueued:
		switch status.State {
		case CASTombstoneFinalOnly, CASTombstoneFinalAndTrash, CASTombstoneTrashOnly:
			status, err = worker.cas.Tombstone(item.Identity.Digest, item.Identity.Token)
			if err != nil {
				return worker.casFailure("close queued CAS object", err)
			}
			if status.State != CASTombstoneTrashOnly || !status.Closed {
				return gcFatal(GCFatalCASInvariant, "CAS tombstone did not close object", nil)
			}
			worker.mu.Lock()
			worker.snapshot.ObjectsTombstoned++
			worker.mu.Unlock()
		case CASTombstoneAbsent:
			return gcFatal(GCFatalCASInvariant, "queued CAS object is absent before rename mark", nil)
		default:
			return gcFatal(GCFatalCASInvariant, "unknown queued CAS state", nil)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		marked, markErr := worker.store.MarkArtifactGCRenamed(ctx,
			GCQueueTransitionSpec{Identity: item.Identity, At: at})
		if markErr != nil {
			if gcContextCancellation(markErr, ctx) {
				return ctx.Err()
			}
			return worker.storeFailure("mark CAS object renamed", markErr)
		}
		if marked.State != GCQueueRenamed || marked.Completed || marked.At != at {
			return gcFatal(GCFatalStoreInvariant, "invalid renamed transition result", nil)
		}
		item.State = GCQueueRenamed
	case GCQueueRenamed:
		if status.State == CASTombstoneFinalOnly || status.State == CASTombstoneFinalAndTrash {
			return gcFatal(GCFatalCASInvariant, "renamed queue item still has an open final object", nil)
		}
	default:
		return gcFatal(GCFatalStoreInvariant, "unknown durable collection queue state", nil)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if status.State == CASTombstoneTrashOnly {
		status, err = worker.cas.PurgeTombstone(item.Identity.Digest, item.Identity.Token)
		if err != nil {
			return worker.casFailure("purge renamed CAS tombstone", err)
		}
		if status.State != CASTombstoneAbsent || !status.Closed {
			return gcFatal(GCFatalCASInvariant, "CAS tombstone purge did not close", nil)
		}
		worker.mu.Lock()
		worker.snapshot.TombstonesPurged++
		worker.mu.Unlock()
	} else if status.State != CASTombstoneAbsent {
		return gcFatal(GCFatalCASInvariant, "renamed queue item has invalid CAS state", nil)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	completed, completeErr := worker.store.CompleteArtifactGC(ctx,
		GCQueueTransitionSpec{Identity: item.Identity, At: at})
	if completeErr != nil {
		if gcContextCancellation(completeErr, ctx) {
			return ctx.Err()
		}
		return worker.storeFailure("complete durable collection queue", completeErr)
	}
	if completed.State != "" || !completed.Completed || completed.At != at {
		return gcFatal(GCFatalStoreInvariant, "invalid collection completion result", nil)
	}
	worker.mu.Lock()
	worker.snapshot.QueueItemsCompleted++
	worker.mu.Unlock()
	return nil
}

func (worker *GCWorker) newToken() ([32]byte, error) {
	var token [32]byte
	for attempt := 0; attempt < gcRandomTokenAttempts; attempt++ {
		if _, err := io.ReadFull(worker.random, token[:]); err != nil {
			return [32]byte{}, err
		}
		if token != ([32]byte{}) {
			return token, nil
		}
	}
	return [32]byte{}, errors.New("random source returned only zero tokens")
}

func (worker *GCWorker) usable(ctx context.Context) error {
	if worker == nil || worker.store == nil || worker.cas == nil || worker.clock == nil ||
		worker.random == nil || ctx == nil {
		return fmt.Errorf("%w: worker is unavailable", ErrGC)
	}
	return nil
}

// gcContextCancellation gives a closed invariant precedence over a concurrent
// cancellation. Only an error that actually carries the caller's cancellation
// is treated as a clean stop; an unrelated Store/CAS failure that races with
// ctx.Done remains fatal evidence.
func gcContextCancellation(err error, ctx context.Context) bool {
	if err == nil || ctx == nil || ctx.Err() == nil ||
		errors.Is(err, ErrGCInvariant) || errors.Is(err, ErrGCStoreInvariant) ||
		errors.Is(err, ErrCASInput) || errors.Is(err, ErrCASCorruption) {
		return false
	}
	return errors.Is(err, ctx.Err()) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (worker *GCWorker) recordFailure(err error) error {
	if err == nil {
		return nil
	}
	worker.mu.Lock()
	worker.snapshot.State = GCFailed
	worker.snapshot.FatalCode = gcFatalCode(err)
	worker.mu.Unlock()
	return err
}

func (worker *GCWorker) storeFailure(operation string, cause error) error {
	code := GCFatalStoreFailure
	if errors.Is(cause, ErrGCStoreInvariant) {
		code = GCFatalStoreInvariant
	}
	return gcFatal(code, operation, cause)
}

func (worker *GCWorker) casFailure(operation string, cause error) error {
	code := GCFatalCASFailure
	if errors.Is(cause, ErrCASInput) || errors.Is(cause, ErrCASCorruption) ||
		errors.Is(cause, os.ErrNotExist) {
		code = GCFatalCASInvariant
	}
	return gcFatal(code, operation, cause)
}

type gcFatalError struct {
	code      GCFatalCode
	operation string
}

func (failure *gcFatalError) Error() string {
	if failure == nil {
		return ErrGCInvariant.Error()
	}
	return fmt.Sprintf("%s: %s", ErrGCInvariant, failure.operation)
}

func (failure *gcFatalError) Unwrap() error { return ErrGCInvariant }

func gcFatal(code GCFatalCode, operation string, cause error) error {
	// The worker's returned error is an operational projection and may reach
	// daemon logs or doctor output. Never retain a Store/CAS cause here: those
	// errors may contain local paths, tombstone names, digests, or GC tokens.
	// Classification is completed before this boundary.
	_ = cause
	return &gcFatalError{code: code, operation: operation}
}

func gcFatalCode(err error) GCFatalCode {
	var failure *gcFatalError
	if errors.As(err, &failure) && failure.code != GCFatalNone {
		return failure.code
	}
	return GCFatalWorkerInvariant
}

func canonicalGCTime(value time.Time) (time.Time, error) {
	value = value.Round(0).UTC()
	if value.IsZero() || value.Year() < 1 || value.Year() > 9999 ||
		!time.Unix(0, value.UnixNano()).UTC().Equal(value) {
		return time.Time{}, errors.New("unsupported Artifact GC time")
	}
	return value, nil
}

func validateGCScanCursor(cursor GCScanCursor, maximumCutoff time.Time, allowDone bool) error {
	cutoff, err := canonicalGCTime(cursor.Cutoff)
	if err != nil || cutoff != cursor.Cutoff || cutoff.After(maximumCutoff) || (!allowDone && cursor.Done) {
		return errors.New("noncanonical or invalid durable scan cursor")
	}
	return nil
}

func validateGCObjectPage(page CASObjectPage, current GCScanCursor, limit int) error {
	if len(page.Candidates) > limit || page.NextCursor.Cutoff() != current.Cutoff ||
		(page.Done && len(page.Candidates) == 0 && page.NextCursor.After() != current.After) ||
		(!page.Done && len(page.Candidates) == 0) {
		return errors.New("invalid CAS object page envelope")
	}
	previous := current.After
	for _, candidate := range page.Candidates {
		if candidate.Digest.IsZero() || (!previous.IsZero() &&
			candidate.Digest.String() <= previous.String()) ||
			!candidate.ModifiedAt.Before(current.Cutoff) || candidate.Size > maxCASObjectSize {
			return errors.New("invalid or unordered CAS object candidate")
		}
		previous = candidate.Digest
	}
	if page.NextCursor.After() != previous {
		return errors.New("CAS page cursor does not follow returned candidates")
	}
	return nil
}

func validateGCPrepareResult(result GCPrepareResult, current GCScanCursor,
	candidates []GCCandidate, pageDone bool, maxQueued int, maxBytes uint64,
) error {
	if result.Examined < 0 || result.Examined > len(candidates) || result.Protected < 0 ||
		result.Queued < 0 || result.Queued > maxQueued ||
		result.Protected+result.Queued != result.Examined || result.Next.Cutoff != current.Cutoff ||
		result.QueuedBytes > maxBytes {
		return errors.New("invalid preparation counters or cutoff")
	}
	expectedAfter := current.After
	var examinedBytes uint64
	for index := 0; index < result.Examined; index++ {
		candidate := candidates[index]
		modified, err := canonicalGCTime(candidate.ModifiedAt)
		if candidate.Digest.IsZero() || candidate.Token == ([32]byte{}) ||
			err != nil || modified != candidate.ModifiedAt || !modified.Before(current.Cutoff) ||
			(!expectedAfter.IsZero() && candidate.Digest.String() <= expectedAfter.String()) {
			return errors.New("invalid prepared candidate identity, age, or order")
		}
		expectedAfter = candidates[index].Digest
		if examinedBytes > maxBytes-candidates[index].SizeBytes {
			return errors.New("preparation examined byte overflow")
		}
		examinedBytes += candidates[index].SizeBytes
	}
	if result.Next.After != expectedAfter || result.QueuedBytes > examinedBytes {
		return errors.New("preparation cursor or byte count differs from examined prefix")
	}
	expectedDone := pageDone && result.Examined == len(candidates)
	if result.Next.Done != expectedDone || (len(candidates) > 0 && result.Examined == 0) {
		return errors.New("preparation did not advance or close its exact page")
	}
	return nil
}

func validateGCStagingScanCursor(cursor GCStagingScanCursor, maximumCutoff time.Time,
	allowDone bool,
) error {
	cutoff, err := canonicalGCTime(cursor.Cutoff)
	if err != nil || cutoff != cursor.Cutoff || cutoff.After(maximumCutoff) ||
		cursor.Generation == 0 || cursor.Generation > gcMaximumStagingPass ||
		(!allowDone && cursor.Done) {
		return errors.New("noncanonical or invalid durable staging scan cursor")
	}
	return nil
}

func validateGCStagingSweep(result GCStagingSweepResult, current GCStagingScanCursor,
	maxItems int, maxBytes uint64,
) error {
	if result.Examined < 0 || result.Examined > maxItems || result.Swept < 0 ||
		result.Swept > result.Examined || result.SweptBytes > maxBytes ||
		result.Next.Generation != current.Generation || result.Next.Cutoff != current.Cutoff ||
		(!current.After.IsZero() && (result.Next.After.IsZero() ||
			result.Next.After.String() < current.After.String())) ||
		(result.Examined == 0 && result.Next.After != current.After) ||
		(result.Examined > 0 && result.Next.After == current.After) {
		return errors.New("invalid bounded staging sweep result")
	}
	return nil
}

func validateGCQueue(items []GCQueueItem, at time.Time) error {
	for index, item := range items {
		if err := validateGCQueueItem(item, at); err != nil {
			return err
		}
		if index > 0 && !gcQueueIdentityLess(items[index-1].Identity, item.Identity) {
			return errors.New("durable collection queue is not strictly ordered")
		}
	}
	return nil
}

func validateGCQueueItem(item GCQueueItem, at time.Time) error {
	if item.Identity.Digest.IsZero() || item.Identity.Token == ([32]byte{}) ||
		item.SizeBytes > maxCASObjectSize ||
		(item.State != GCQueueQueued && item.State != GCQueueRenamed) {
		return errors.New("invalid durable collection queue item")
	}
	modified, modifiedErr := canonicalGCTime(item.ModifiedAt)
	queued, queuedErr := canonicalGCTime(item.QueuedAt)
	if modifiedErr != nil || queuedErr != nil || modified != item.ModifiedAt || queued != item.QueuedAt ||
		!modified.Before(queued) || queued.After(at) {
		return errors.New("invalid durable collection queue times")
	}
	if item.State == GCQueueQueued && !item.RenamedAt.IsZero() {
		return errors.New("queued collection item has a rename time")
	}
	if item.State == GCQueueRenamed {
		renamed, err := canonicalGCTime(item.RenamedAt)
		if err != nil || renamed != item.RenamedAt || renamed.Before(queued) || renamed.After(at) {
			return errors.New("renamed collection item has invalid transition time")
		}
	}
	return nil
}

func validateGCTombstoneDescriptor(descriptor CASTombstoneDescriptor) error {
	if descriptor.Digest.IsZero() || descriptor.Token == ([32]byte{}) {
		return errors.New("incomplete CAS tombstone descriptor")
	}
	return validateGCTombstoneStatus(CASTombstoneStatus{
		State: descriptor.State, Closed: descriptor.Closed,
	})
}

func validateGCTombstoneStatus(status CASTombstoneStatus) error {
	switch status.State {
	case CASTombstoneFinalOnly, CASTombstoneFinalAndTrash:
		if status.Closed {
			return errors.New("open CAS tombstone state marked closed")
		}
	case CASTombstoneTrashOnly, CASTombstoneAbsent:
		if !status.Closed {
			return errors.New("closed CAS tombstone state marked open")
		}
	default:
		return errors.New("unknown CAS tombstone state")
	}
	return nil
}

func gcQueueIdentityLess(left, right GCQueueIdentity) bool {
	if left.Digest != right.Digest {
		return left.Digest.String() < right.Digest.String()
	}
	return bytes.Compare(left.Token[:], right.Token[:]) < 0
}

func gcQueueKey(identity GCQueueIdentity) string {
	return identity.Digest.String() + "\x00" + string(identity.Token[:])
}
