package peer

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const (
	artifactReceiverDefaultPeriod     = 10 * time.Second
	artifactReceiverRenewalWindow     = 60 * time.Second
	artifactReceiverRetryMinimum      = time.Second
	artifactReceiverRetryMaximum      = 300 * time.Second
	artifactReceiverOwnerRandomBytes  = 12
	artifactReceiverResponseLossProbe = 2
)

var (
	ErrArtifactReceiver                 = errors.New("Mnemon Artifact receiver")
	ErrArtifactReceiverRunning          = fmt.Errorf("%w: worker has already run", ErrArtifactReceiver)
	ErrArtifactReceiverInvariant        = fmt.Errorf("%w: invariant failed", ErrArtifactReceiver)
	errArtifactReceiverAuthorityChanged = errors.New("Artifact receiver authority changed")
	errArtifactReceiverClaimStale       = errors.New("Artifact receiver claim is stale")
)

// ArtifactReceiverStore is the complete durable surface used by the Artifact
// receiver. Calls are intentionally narrow: no network or CAS operation can
// occur while one of these methods owns a Store transaction.
type ArtifactReceiverStore interface {
	ClaimPeerInboxArtifact(context.Context, store.ClaimPeerInboxArtifactSpec) (store.PeerInboxArtifactClaimResult, error)
	RenewPeerInboxArtifactLease(context.Context, store.RenewPeerInboxArtifactSpec) (store.PeerInboxArtifactRenewal, error)
	ProbePeerInboxArtifactAuthority(context.Context, store.ProbePeerInboxArtifactAuthoritySpec) error
	ReadPeerInboxArtifactRoot(context.Context, store.ReadPeerInboxArtifactRootSpec) (store.PeerInboxArtifactRoot, bool, error)
	RecordPeerInboxArtifactSource(context.Context, store.RecordPeerInboxArtifactSourceSpec) (store.PeerInboxArtifactSourceReceipt, error)
	BeginPeerInboxArtifactStage(context.Context, store.BeginPeerInboxArtifactStageSpec) (store.PeerInboxArtifactStageRegistration, error)
	PreparePeerInboxArtifactPublish(context.Context, store.PreparePeerInboxArtifactPublishSpec) (store.PeerInboxArtifactStage, error)
	AcceptPeerInboxArtifactPublish(context.Context, store.AcceptPeerInboxArtifactPublishSpec) (store.PeerInboxArtifactStage, error)
	ReadPeerInboxArtifactPublish(context.Context, store.ReadPeerInboxArtifactPublishSpec) (store.PeerInboxArtifactPublishCheckpoint, error)
	MarkPeerInboxArtifactReady(context.Context, store.MarkPeerInboxArtifactReadySpec) (store.PeerInboxArtifactSettlement, error)
	RetryPeerInboxArtifact(context.Context, store.RetryPeerInboxArtifactSpec) (store.PeerInboxArtifactSettlement, error)
	QuarantinePeerInboxArtifact(context.Context, store.QuarantinePeerInboxArtifactSpec) (store.PeerInboxArtifactSettlement, error)
}

type ArtifactReceiverClient interface {
	GetManifest(context.Context, model.PeerID, GetManifest) (Manifest, error)
	GetBlock(context.Context, model.PeerID, GetBlock) (Block, error)
}

// ArtifactReceiverReconciler wakes the independent Channel authority path.
// It cannot authorize Artifact bytes or settle the claimed Inbox row.
type ArtifactReceiverReconciler interface {
	ReconcileArtifactReceiver(context.Context, model.ChannelID, model.PeerID) error
}

type ArtifactReceiverClock interface{ Now() time.Time }

type ArtifactReceiverOptions struct {
	Store      ArtifactReceiverStore
	Client     ArtifactReceiverClient
	CAS        *artifactdomain.CAS
	Reconciler ArtifactReceiverReconciler
	Clock      ArtifactReceiverClock
	Period     time.Duration
}

type ArtifactReceiverState string

const (
	ArtifactReceiverIdle    ArtifactReceiverState = "idle"
	ArtifactReceiverRunning ArtifactReceiverState = "running"
	ArtifactReceiverStopped ArtifactReceiverState = "stopped"
	ArtifactReceiverFailed  ArtifactReceiverState = "failed"
)

type ArtifactReceiverFatalCode string

const (
	ArtifactReceiverFatalNone            ArtifactReceiverFatalCode = ""
	ArtifactReceiverFatalStoreInvariant  ArtifactReceiverFatalCode = "store_invariant"
	ArtifactReceiverFatalStoreFailure    ArtifactReceiverFatalCode = "store_failure"
	ArtifactReceiverFatalCASInvariant    ArtifactReceiverFatalCode = "cas_invariant"
	ArtifactReceiverFatalCASFailure      ArtifactReceiverFatalCode = "cas_failure"
	ArtifactReceiverFatalClientInvariant ArtifactReceiverFatalCode = "client_invariant"
	ArtifactReceiverFatalWorkerInvariant ArtifactReceiverFatalCode = "worker_invariant"
)

// ArtifactReceiverSnapshot is a closed operational projection. It carries no
// remote text, paths, digests, peer identities, or implementation errors.
type ArtifactReceiverSnapshot struct {
	State                  ArtifactReceiverState
	FatalCode              ArtifactReceiverFatalCode
	Cycles                 uint64
	ClaimScans             uint64
	Claims                 uint64
	Ready                  uint64
	Retries                uint64
	Quarantines            uint64
	StaleClaims            uint64
	Renewals               uint64
	ManifestCacheHits      uint64
	ManifestPulls          uint64
	BlockCacheHits         uint64
	BlockPulls             uint64
	Checkpoints            uint64
	Reconciliations        uint64
	ReconciliationFailures uint64
	InFlightClaims         int
	MaximumInFlightClaims  int
	InFlightPulls          int
	MaximumInFlightPulls   int
	InClosureBuild         int
	MaximumInClosureBuild  int
	LastCycleAt            time.Time
}

type ArtifactReceiver struct {
	backend    artifactReceiverBackend
	client     ArtifactReceiverClient
	cas        artifactReceiverCAS
	reconciler ArtifactReceiverReconciler
	clock      ArtifactReceiverClock
	period     time.Duration
	owner      string
	workers    int
	pulls      *artifactReceiverPullLimiter
	closure    chan struct{}
	trigger    chan struct{}

	mu       sync.Mutex
	snapshot ArtifactReceiverSnapshot
	started  bool
	running  bool
}

type artifactReceiverCAS interface {
	OpenStage(artifactdomain.StageOwner) (*artifactdomain.Stage, error)
	Read(model.Digest, int) ([]byte, error)
}

var (
	_ ArtifactReceiverStore  = (*store.Store)(nil)
	_ ArtifactReceiverClient = (*ArtifactClient)(nil)
	_ artifactReceiverCAS    = (*artifactdomain.CAS)(nil)
)

type artifactReceiverFence struct {
	inboxID    model.InboxID
	leaseOwner string
	leaseUntil time.Time
	attempt    uint32
	durable    store.PeerInboxArtifactFence
	hasDurable bool
	token      uint64
}

type artifactReceiverClaim struct {
	fence         artifactReceiverFence
	publication   model.SignedPublication
	channelID     model.ChannelID
	originPeerID  model.PeerID
	originEpoch   model.OriginEpoch
	requiredRoots []model.Digest
}

type artifactReceiverBackend interface {
	claim(context.Context, string, time.Time) (artifactReceiverClaim, bool, error)
	renew(context.Context, artifactReceiverFence, time.Time) (artifactReceiverFence, error)
	probe(context.Context, artifactReceiverFence, time.Time) error
	readRoot(context.Context, artifactReceiverFence, model.Digest, time.Time) (artifactReceiverCachedRoot, bool, error)
	recordSource(context.Context, artifactReceiverFence, model.PeerID, time.Time) error
	beginStage(context.Context, artifactReceiverFence, time.Time) (artifactReceiverStageRegistration, error)
	preparePublish(context.Context, artifactReceiverFence, artifactdomain.StageOwner, store.VerifiedArtifactClosure, time.Time) error
	acceptPublish(context.Context, artifactReceiverFence, artifactdomain.StageOwner, time.Time) error
	readPublish(context.Context, artifactReceiverFence, artifactdomain.StageOwner, time.Time) (artifactReceiverPublishCheckpoint, error)
	ready(context.Context, artifactReceiverFence, artifactdomain.StageOwner, time.Time) error
	retry(context.Context, artifactReceiverFence, store.PeerInboxArtifactRetryDiagnostic, time.Duration, time.Time) error
	quarantine(context.Context, artifactReceiverFence, store.PeerInboxArtifactPermanentDiagnostic, time.Time) error
}

type durableArtifactReceiverBackend struct{ store ArtifactReceiverStore }

type wallArtifactReceiverClock struct{}

func (wallArtifactReceiverClock) Now() time.Time { return time.Now() }

type artifactReceiverConfig struct {
	backend       artifactReceiverBackend
	client        ArtifactReceiverClient
	cas           artifactReceiverCAS
	reconciler    ArtifactReceiverReconciler
	clock         ArtifactReceiverClock
	period        time.Duration
	owner         string
	workers       int
	nodePulls     int
	peerPulls     int
	closureBuilds int
}

func NewArtifactReceiver(options ArtifactReceiverOptions) (*ArtifactReceiver, error) {
	if options.Store == nil || options.Client == nil || options.CAS == nil || options.Reconciler == nil {
		return nil, fmt.Errorf("%w: Store, client, CAS and reconciler are required", ErrArtifactReceiver)
	}
	clock := options.Clock
	if clock == nil {
		clock = wallArtifactReceiverClock{}
	}
	period := options.Period
	if period == 0 {
		period = artifactReceiverDefaultPeriod
	}
	owner, err := newArtifactReceiverOwner()
	if err != nil {
		return nil, fmt.Errorf("%w: generate lease owner: %v", ErrArtifactReceiver, err)
	}
	limits := HermeticLimits()
	return newArtifactReceiver(artifactReceiverConfig{
		backend: durableArtifactReceiverBackend{store: options.Store}, client: options.Client,
		cas: options.CAS, reconciler: options.Reconciler, clock: clock, period: period,
		owner: owner, workers: limits.InboxWorkers, nodePulls: limits.NodeArtifactPulls,
		peerPulls: limits.PeerArtifactPulls, closureBuilds: 1,
	})
}

func newArtifactReceiver(config artifactReceiverConfig) (*ArtifactReceiver, error) {
	limits := HermeticLimits()
	if config.backend == nil || config.client == nil || config.cas == nil || config.reconciler == nil ||
		config.clock == nil || config.period <= 0 || config.period > artifactReceiverDefaultPeriod ||
		config.owner == "" || len(config.owner) > 512 || config.workers <= 0 ||
		config.workers > limits.InboxWorkers || config.nodePulls <= 0 ||
		config.nodePulls > limits.NodeArtifactPulls || config.peerPulls <= 0 ||
		config.peerPulls > limits.PeerArtifactPulls || config.peerPulls > config.nodePulls ||
		config.closureBuilds != 1 {
		return nil, fmt.Errorf("%w: complete Hermetic worker configuration is required", ErrArtifactReceiver)
	}
	pulls, err := newArtifactReceiverPullLimiter(config.nodePulls, config.peerPulls)
	if err != nil {
		return nil, err
	}
	return &ArtifactReceiver{
		backend: config.backend, client: config.client, cas: config.cas,
		reconciler: config.reconciler, clock: config.clock, period: config.period,
		owner: config.owner, workers: config.workers, pulls: pulls,
		closure: make(chan struct{}, config.closureBuilds), trigger: make(chan struct{}, 1),
		snapshot: ArtifactReceiverSnapshot{State: ArtifactReceiverIdle},
	}, nil
}

func newArtifactReceiverOwner() (string, error) {
	random := make([]byte, artifactReceiverOwnerRandomBytes)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return "artifact-receiver-" + hex.EncodeToString(random), nil
}

// Trigger coalesces any number of wakeups into one future durable scan.
func (receiver *ArtifactReceiver) Trigger() {
	if receiver == nil || receiver.trigger == nil {
		return
	}
	select {
	case receiver.trigger <- struct{}{}:
	default:
	}
}

func (receiver *ArtifactReceiver) Snapshot() ArtifactReceiverSnapshot {
	if receiver == nil {
		return ArtifactReceiverSnapshot{State: ArtifactReceiverFailed,
			FatalCode: ArtifactReceiverFatalWorkerInvariant}
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	return receiver.snapshot
}

// Run scans immediately at startup and then at least once per ten seconds.
// A receiver is deliberately single-use: starting it twice is a fail-closed
// lifecycle error rather than a second consumer of the same durable queue.
func (receiver *ArtifactReceiver) Run(ctx context.Context) error {
	if receiver == nil || receiver.backend == nil || receiver.client == nil || receiver.cas == nil ||
		receiver.reconciler == nil || receiver.clock == nil || receiver.pulls == nil || ctx == nil {
		return fmt.Errorf("%w: worker is unavailable", ErrArtifactReceiver)
	}
	receiver.mu.Lock()
	if receiver.started || receiver.running {
		receiver.mu.Unlock()
		return ErrArtifactReceiverRunning
	}
	receiver.started = true
	receiver.running = true
	receiver.snapshot.State = ArtifactReceiverRunning
	receiver.snapshot.FatalCode = ArtifactReceiverFatalNone
	receiver.mu.Unlock()
	failed := false
	defer func() {
		receiver.mu.Lock()
		receiver.running = false
		if !failed {
			receiver.snapshot.State = ArtifactReceiverStopped
		}
		receiver.mu.Unlock()
	}()

	ticker := time.NewTicker(receiver.period)
	defer ticker.Stop()
	for {
		if err := receiver.runCycle(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failed = true
			receiver.fail(artifactReceiverFatalCode(err))
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-receiver.trigger:
		}
	}
}

func (receiver *ArtifactReceiver) runCycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	at, err := receiver.now()
	if err != nil {
		return artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant, "read cycle clock", err)
	}
	receiver.recordCycle(at)
	cycleContext, cancel := context.WithCancel(ctx)
	defer cancel()
	fatal := make(chan error, 1)
	var wait sync.WaitGroup
	for worker := 0; worker < receiver.workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := receiver.claimLoop(cycleContext); err != nil {
				select {
				case fatal <- err:
					cancel()
				default:
				}
			}
		}()
	}
	wait.Wait()
	select {
	case err := <-fatal:
		return err
	default:
	}
	return ctx.Err()
}

func (receiver *ArtifactReceiver) claimLoop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		at, err := receiver.now()
		if err != nil {
			return artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant, "read claim clock", err)
		}
		receiver.recordClaimScan()
		claim, found, err := receiver.backend.claim(ctx, receiver.owner, at)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, store.ErrPeerInboxArtifactStale) {
				receiver.recordStale()
				return nil
			}
			return receiver.storeFatal("claim Inbox Artifact", err)
		}
		if !found {
			return nil
		}
		if err := validateArtifactReceiverClaim(claim, receiver.owner, at); err != nil {
			return artifactReceiverFatal(ArtifactReceiverFatalStoreInvariant,
				"invalid durable claim", err)
		}
		receiver.claimStarted()
		err = receiver.processClaim(ctx, &claim)
		receiver.claimFinished()
		if err != nil {
			return err
		}
	}
}

func (receiver *ArtifactReceiver) processClaim(ctx context.Context,
	claim *artifactReceiverClaim,
) error {
	failure, err := receiver.receiveClaim(ctx, claim)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		switch {
		case errors.Is(err, errArtifactReceiverClaimStale):
			return nil
		case errors.Is(err, errArtifactReceiverAuthorityChanged):
			failure = retryArtifactReceiverClaim(store.PeerInboxArtifactRetryNotAuthorized,
				artifactReceiverRetryMinimum)
			failure.reconcile = true
		default:
			return err
		}
	}
	if failure == nil {
		return nil
	}
	if ctx.Err() != nil {
		return nil
	}
	settleErr := receiver.settleFailure(ctx, claim, *failure)
	if !errors.Is(settleErr, errArtifactReceiverAuthorityChanged) {
		return settleErr
	}
	// Authority can disappear while a non-authority failure is waiting to
	// settle. Convert that race into the same bounded not-authorized path used
	// by a pre-RPC probe; it must not escape as a fatal worker error.
	authorityFailure := retryArtifactReceiverClaim(
		store.PeerInboxArtifactRetryNotAuthorized, artifactReceiverRetryMinimum)
	authorityFailure.reconcile = true
	return receiver.settleFailure(ctx, claim, *authorityFailure)
}

func (receiver *ArtifactReceiver) receiveClaim(ctx context.Context,
	claim *artifactReceiverClaim,
) (*artifactReceiverClaimFailure, error) {
	if len(claim.requiredRoots) == 0 {
		return receiver.markReady(ctx, claim, artifactdomain.StageOwner{})
	}
	registration, failure, err := receiver.beginArtifactStage(ctx, claim)
	if err != nil || failure != nil {
		return failure, err
	}
	stage, err := receiver.cas.OpenStage(registration.owner)
	if err != nil {
		return receiver.classifyArtifactStageFailure(
			"open Inbox Artifact stage", claim.fence.attempt, err)
	}
	if registration.state == store.ArtifactStagePublishing {
		return receiver.resumeArtifactPublish(ctx, claim, registration, stage)
	}
	if registration.state != store.ArtifactStageStaged {
		return nil, artifactReceiverFatal(ArtifactReceiverFatalStoreInvariant,
			"begin Inbox Artifact stage returned an invalid state", nil)
	}
	return receiver.receiveStagedClaim(ctx, claim, stage)
}

// buildCollectedClosure is the only memory-bounded closure-build section.
// Network block pulls, CAS verification and Store settlement happen after the
// singleton gate is released so one slow peer cannot head-of-line block an
// unrelated Inbox claim.
func (receiver *ArtifactReceiver) buildCollectedClosure(ctx context.Context,
	claim *artifactReceiverClaim, stage *artifactdomain.Stage,
	refs []artifactReceiverManifestRef,
) (artifactdomain.Closure, *artifactReceiverClaimFailure, error) {
	live, err := receiver.ensureLease(ctx, claim)
	if err != nil || !live {
		return artifactdomain.Closure{}, nil, err
	}
	select {
	case receiver.closure <- struct{}{}:
		receiver.closureStarted()
		defer func() {
			receiver.closureFinished()
			<-receiver.closure
		}()
	case <-ctx.Done():
		return artifactdomain.Closure{}, nil, nil
	}
	live, err = receiver.ensureLease(ctx, claim)
	if err != nil || !live {
		return artifactdomain.Closure{}, nil, err
	}
	manifests, failure, err := receiver.loadCollectedManifests(stage, refs)
	if err != nil || failure != nil {
		return artifactdomain.Closure{}, failure, err
	}
	live, err = receiver.ensureLease(ctx, claim)
	if err != nil || !live {
		return artifactdomain.Closure{}, nil, err
	}
	verifiedAt, err := receiver.now()
	if err != nil {
		return artifactdomain.Closure{}, nil, artifactReceiverFatal(
			ArtifactReceiverFatalWorkerInvariant, "read imported closure clock", err)
	}
	closure, buildErr := artifactdomain.BuildImportedClosure(ctx, manifests, verifiedAt)
	if buildErr != nil {
		if ctx.Err() != nil {
			return artifactdomain.Closure{}, nil, nil
		}
		return artifactdomain.Closure{}, classifyImportedClosureFailure(buildErr), nil
	}
	live, err = receiver.ensureLease(ctx, claim)
	if err != nil || !live {
		return artifactdomain.Closure{}, nil, err
	}
	return closure, nil, nil
}

type artifactReceiverManifestRef struct {
	rootDigest     model.Digest
	manifestDigest model.Digest
	verified       bool
}

type artifactReceiverCachedRoot struct {
	rootDigest     model.Digest
	manifest       model.JSON
	manifestDigest model.Digest
	totalBytes     uint64
	createdAt      time.Time
	verifiedAt     time.Time
	verified       bool
}

func (receiver *ArtifactReceiver) fetchManifest(ctx context.Context, claim *artifactReceiverClaim,
	stage *artifactdomain.Stage, rootDigest model.Digest,
) (artifactReceiverManifestRef, *artifactReceiverClaimFailure, error) {
	request, err := NewGetManifest(GetManifestSpec{ChannelID: claim.channelID,
		RootDigest: rootDigest})
	if err != nil {
		return artifactReceiverManifestRef{}, nil, artifactReceiverFatal(
			ArtifactReceiverFatalClientInvariant, "construct GetManifest", err)
	}
	var response Manifest
	failure, err := receiver.pull(ctx, claim, func(requestContext context.Context) error {
		var callErr error
		response, callErr = receiver.client.GetManifest(requestContext, claim.originPeerID, request)
		return callErr
	})
	if err != nil || failure != nil {
		return artifactReceiverManifestRef{}, failure, err
	}
	manifest, parseErr := artifactdomain.ParseManifest(response.ManifestBytes())
	if parseErr != nil {
		return artifactReceiverManifestRef{}, classifyRemoteManifestFailure(parseErr), nil
	}
	if response.RootDigest() != rootDigest || manifest.RootDigest() != rootDigest ||
		response.ManifestDigest() != manifest.ManifestDigest() ||
		!bytes.Equal(response.ManifestBytes(), manifest.CanonicalJSON().Bytes()) {
		return artifactReceiverManifestRef{}, quarantineArtifactReceiverClaim(
			store.PeerInboxArtifactDigestMismatch), nil
	}
	if _, putErr := stage.Put(manifest.ManifestDigest(), manifest.CanonicalJSON().Bytes()); putErr != nil {
		if artifactReceiverResourceFailure(putErr) {
			return artifactReceiverManifestRef{}, retryArtifactReceiverClaim(
				store.PeerInboxArtifactRetryResourceExhausted,
				artifactReceiverBackoff(claim.fence.attempt)), nil
		}
		return artifactReceiverManifestRef{}, nil,
			receiver.casFatal("store imported Artifact manifest", putErr)
	}
	receiver.recordManifestPull()
	return artifactReceiverManifestRef{rootDigest: rootDigest,
		manifestDigest: manifest.ManifestDigest()}, nil, nil
}

func (receiver *ArtifactReceiver) loadCollectedManifests(stage *artifactdomain.Stage,
	refs []artifactReceiverManifestRef,
) ([]artifactdomain.Manifest, *artifactReceiverClaimFailure, error) {
	manifests := make([]artifactdomain.Manifest, 0, len(refs))
	for _, ref := range refs {
		content, err := stage.ReadAvailable(ref.manifestDigest, artifactdomain.MaxManifestBytes)
		if err != nil {
			if artifactReceiverResourceFailure(err) {
				return nil, retryArtifactReceiverClaim(
					store.PeerInboxArtifactRetryResourceExhausted, artifactReceiverRetryMinimum), nil
			}
			return nil, nil, receiver.casFatal("read collected Artifact manifest", err)
		}
		manifest, err := artifactdomain.ParseManifest(content)
		if err != nil {
			return nil, nil, receiver.casFatal("parse collected Artifact manifest", err)
		}
		if manifest.RootDigest() != ref.rootDigest || manifest.ManifestDigest() != ref.manifestDigest {
			return nil, nil, receiver.casFatal("collected Artifact manifest binding differs",
				artifactdomain.ErrCASCorruption)
		}
		manifests = append(manifests, manifest)
	}
	return manifests, nil, nil
}

func (receiver *ArtifactReceiver) materializeBlocks(ctx context.Context,
	claim *artifactReceiverClaim, stage *artifactdomain.Stage, closure artifactdomain.Closure,
	refs []artifactReceiverManifestRef,
) (*artifactReceiverClaimFailure, error) {
	cachedRoots := make(map[model.Digest]bool, len(refs))
	for _, ref := range refs {
		cachedRoots[ref.rootDigest] = ref.verified
	}
	owners := make(map[model.Digest]model.Digest, len(closure.Blocks()))
	verifiedOwners := make(map[model.Digest]bool, len(closure.Blocks()))
	for _, mapping := range closure.BlockMap() {
		if _, found := owners[mapping.BlockDigest]; !found {
			owners[mapping.BlockDigest] = mapping.RootDigest
		}
		if cachedRoots[mapping.RootDigest] {
			verifiedOwners[mapping.BlockDigest] = true
		}
	}
	for _, block := range closure.Blocks() {
		content, readErr := stage.ReadAvailable(block.Digest, artifactdomain.BlockSize)
		if readErr == nil {
			if uint64(len(content)) != block.SizeBytes {
				return nil, receiver.casFatal("cached Artifact block length differs",
					artifactdomain.ErrCASCorruption)
			}
			receiver.recordBlockCacheHit()
			continue
		}
		if artifactReceiverResourceFailure(readErr) {
			return retryArtifactReceiverClaim(store.PeerInboxArtifactRetryResourceExhausted,
				artifactReceiverBackoff(claim.fence.attempt)), nil
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			return nil, receiver.casFatal("read cached Artifact block", readErr)
		}
		if verifiedOwners[block.Digest] {
			return nil, receiver.casFatal("verified Store root has a missing CAS block", readErr)
		}
		owner, found := owners[block.Digest]
		if !found || owner.IsZero() {
			return nil, artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant,
				"Artifact block has no authorizing root", nil)
		}
		request, err := NewGetBlock(GetBlockSpec{ChannelID: claim.channelID,
			RootDigest: owner, BlockDigest: block.Digest})
		if err != nil {
			return nil, artifactReceiverFatal(ArtifactReceiverFatalClientInvariant,
				"construct GetBlock", err)
		}
		var response Block
		failure, err := receiver.pull(ctx, claim, func(requestContext context.Context) error {
			var callErr error
			response, callErr = receiver.client.GetBlock(requestContext, claim.originPeerID, request)
			return callErr
		})
		if err != nil || failure != nil {
			return failure, err
		}
		blockBytes := response.BlockBytes()
		if response.BlockDigest() != block.Digest || uint64(len(blockBytes)) != block.SizeBytes ||
			model.Sum(blockBytes) != block.Digest {
			return quarantineArtifactReceiverClaim(store.PeerInboxArtifactDigestMismatch), nil
		}
		if _, putErr := stage.Put(block.Digest, blockBytes); putErr != nil {
			if artifactReceiverResourceFailure(putErr) {
				return retryArtifactReceiverClaim(store.PeerInboxArtifactRetryResourceExhausted,
					artifactReceiverBackoff(claim.fence.attempt)), nil
			}
			return nil, receiver.casFatal("store imported Artifact block", putErr)
		}
		receiver.recordBlockPull()
	}
	return nil, nil
}

func (receiver *ArtifactReceiver) pull(ctx context.Context, claim *artifactReceiverClaim,
	call func(context.Context) error,
) (*artifactReceiverClaimFailure, error) {
	live, err := receiver.ensureLease(ctx, claim)
	if err != nil || !live {
		return nil, err
	}
	release, err := receiver.pulls.acquire(ctx, claim.originPeerID)
	if err != nil {
		return nil, nil
	}
	receiver.pullStarted()
	defer func() {
		receiver.pullFinished()
		release()
	}()
	live, err = receiver.ensureLease(ctx, claim)
	if err != nil || !live {
		return nil, err
	}
	if err := receiver.probeAuthority(ctx, claim); err != nil {
		return nil, err
	}
	requestContext, cancel := context.WithTimeout(ctx, HermeticLimits().ArtifactRequestTimeout)
	callErr := call(requestContext)
	requestErr := requestContext.Err()
	cancel()
	if ctx.Err() != nil {
		return nil, nil
	}
	live, err = receiver.ensureLease(ctx, claim)
	if err != nil || !live {
		return nil, err
	}
	if errors.Is(requestErr, context.DeadlineExceeded) || errors.Is(callErr, context.DeadlineExceeded) {
		return retryArtifactReceiverClaim(store.PeerInboxArtifactRetryTimeout,
			artifactReceiverBackoff(claim.fence.attempt)), nil
	}
	if callErr == nil {
		return receiver.recordDirectSource(ctx, claim)
	}
	var remote *ArtifactRemoteFailure
	if errors.As(callErr, &remote) {
		switch remote.Code() {
		case ArtifactErrorBusy:
			return retryArtifactReceiverClaim(store.PeerInboxArtifactRetryBusy,
				artifactReceiverClampRetry(remote.RetryAfter())), nil
		case ArtifactErrorNotAuthorized:
			failure := retryArtifactReceiverClaim(store.PeerInboxArtifactRetryNotAuthorized,
				artifactReceiverBackoff(claim.fence.attempt))
			failure.reconcile = true
			return failure, nil
		case ArtifactErrorCorrupt:
			return quarantineArtifactReceiverClaim(store.PeerInboxArtifactDigestMismatch), nil
		default:
			return nil, artifactReceiverFatal(ArtifactReceiverFatalClientInvariant,
				"unknown Artifact remote failure", callErr)
		}
	}
	if errors.Is(callErr, ErrArtifactClientDigestMismatch) {
		return quarantineArtifactReceiverClaim(store.PeerInboxArtifactDigestMismatch), nil
	}
	if errors.Is(callErr, ErrArtifactClientManifestInvalid) {
		return quarantineArtifactReceiverClaim(store.PeerInboxArtifactManifestInvalid), nil
	}
	if errors.Is(callErr, ErrArtifactClientResponse) {
		return quarantineArtifactReceiverClaim(store.PeerInboxArtifactProtocolInvalid), nil
	}
	if errors.Is(callErr, ErrArtifactClientTransport) || errors.Is(callErr, io.EOF) ||
		errors.Is(callErr, net.ErrClosed) || errors.Is(callErr, syscall.ECONNRESET) ||
		errors.Is(callErr, syscall.ECONNABORTED) || errors.Is(callErr, syscall.EPIPE) ||
		errors.Is(callErr, context.Canceled) {
		return retryArtifactReceiverClaim(store.PeerInboxArtifactRetryTransportUnavailable,
			artifactReceiverBackoff(claim.fence.attempt)), nil
	}
	return nil, artifactReceiverFatal(ArtifactReceiverFatalClientInvariant,
		"unexpected Artifact client failure", callErr)
}

func (receiver *ArtifactReceiver) recordDirectSource(ctx context.Context,
	claim *artifactReceiverClaim,
) (*artifactReceiverClaimFailure, error) {
	at, err := receiver.now()
	if err != nil {
		return nil, artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant,
			"read Artifact source receipt clock", err)
	}
	if err := receiver.backend.recordSource(ctx, claim.fence,
		claim.originPeerID, at); err != nil {
		return receiver.classifyStoreClaimFailure("record Artifact direct source", err)
	}
	return nil, nil
}

// probeAuthority is the last local gate before opening an Artifact stream. It
// deliberately does not extend the lease: every remote pull must re-observe
// the exact claim generation and current Channel authority after waiting on
// the node and peer limiters.
func (receiver *ArtifactReceiver) probeAuthority(ctx context.Context,
	claim *artifactReceiverClaim,
) error {
	at, err := receiver.now()
	if err != nil {
		return artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant,
			"read Artifact authority clock", err)
	}
	err = receiver.backend.probe(ctx, claim.fence, at)
	if err == nil || ctx.Err() != nil {
		return nil
	}
	if errors.Is(err, store.ErrPeerInboxArtifactStale) {
		receiver.recordStale()
		return errArtifactReceiverClaimStale
	}
	if errors.Is(err, store.ErrPeerInboxArtifactAuthority) {
		return errArtifactReceiverAuthorityChanged
	}
	return receiver.storeFatal("probe Inbox Artifact authority", err)
}

func (receiver *ArtifactReceiver) ensureLease(ctx context.Context,
	claim *artifactReceiverClaim,
) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	at, err := receiver.now()
	if err != nil {
		return false, artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant,
			"read lease clock", err)
	}
	if claim.fence.leaseUntil.Sub(at) > artifactReceiverRenewalWindow {
		return true, nil
	}
	var renewed artifactReceiverFence
	var renewErr error
	for probe := 0; probe < artifactReceiverResponseLossProbe; probe++ {
		renewed, renewErr = receiver.backend.renew(ctx, claim.fence, at)
		if renewErr == nil || ctx.Err() != nil || artifactReceiverClosedStoreError(renewErr) {
			break
		}
	}
	if renewErr != nil {
		if ctx.Err() != nil {
			return false, nil
		}
		if errors.Is(renewErr, store.ErrPeerInboxArtifactStale) {
			receiver.recordStale()
			return false, nil
		}
		if errors.Is(renewErr, store.ErrPeerInboxArtifactAuthority) {
			return false, errArtifactReceiverAuthorityChanged
		}
		return false, receiver.storeFatal("renew Inbox Artifact lease", renewErr)
	}
	if !sameArtifactReceiverFenceGeneration(claim.fence, renewed) ||
		!renewed.leaseUntil.After(at.Add(artifactReceiverRenewalWindow)) {
		return false, artifactReceiverFatal(ArtifactReceiverFatalStoreInvariant,
			"invalid renewed Inbox Artifact fence", nil)
	}
	claim.fence = renewed
	receiver.recordRenewal()
	return true, nil
}

func (receiver *ArtifactReceiver) markReady(ctx context.Context,
	claim *artifactReceiverClaim, owner artifactdomain.StageOwner,
) (*artifactReceiverClaimFailure, error) {
	if owner.IsZero() {
		live, err := receiver.ensureLease(ctx, claim)
		if err != nil || !live {
			return nil, err
		}
	}
	at, err := receiver.now()
	if err != nil {
		return nil, artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant,
			"read ready clock", err)
	}
	var readyErr error
	for probe := 0; probe < artifactReceiverResponseLossProbe; probe++ {
		readyErr = receiver.backend.ready(ctx, claim.fence, owner, at)
		if readyErr == nil || ctx.Err() != nil || artifactReceiverClosedStoreError(readyErr) {
			break
		}
	}
	if readyErr == nil {
		receiver.recordReady()
		return nil, nil
	}
	return receiver.classifyStoreClaimFailure("mark Inbox Artifact ready", readyErr)
}

type artifactReceiverClaimFailure struct {
	retry      store.PeerInboxArtifactRetryDiagnostic
	permanent  store.PeerInboxArtifactPermanentDiagnostic
	retryAfter time.Duration
	reconcile  bool
}

func retryArtifactReceiverClaim(diagnostic store.PeerInboxArtifactRetryDiagnostic,
	after time.Duration,
) *artifactReceiverClaimFailure {
	return &artifactReceiverClaimFailure{retry: diagnostic,
		retryAfter: artifactReceiverClampRetry(after)}
}

func quarantineArtifactReceiverClaim(diagnostic store.PeerInboxArtifactPermanentDiagnostic,
) *artifactReceiverClaimFailure {
	return &artifactReceiverClaimFailure{permanent: diagnostic}
}

func (receiver *ArtifactReceiver) settleFailure(ctx context.Context, claim *artifactReceiverClaim,
	failure artifactReceiverClaimFailure,
) error {
	if ctx.Err() != nil {
		return nil
	}
	notAuthorized := failure.retry == store.PeerInboxArtifactRetryNotAuthorized &&
		failure.permanent == ""
	if !notAuthorized {
		live, err := receiver.ensureLease(ctx, claim)
		if err != nil || !live {
			return err
		}
	}
	if failure.reconcile {
		reconcileContext, cancel := context.WithTimeout(ctx, HermeticLimits().ChannelRequestTimeout)
		reconcileErr := receiver.reconciler.ReconcileArtifactReceiver(reconcileContext,
			claim.channelID, claim.originPeerID)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		receiver.recordReconciliation(reconcileErr == nil)
		if !notAuthorized {
			live, err := receiver.ensureLease(ctx, claim)
			if err != nil || !live {
				return err
			}
		}
	}
	at, err := receiver.now()
	if err != nil {
		return artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant,
			"read settlement clock", err)
	}
	var settle func() error
	if failure.retry.Valid() && failure.permanent == "" {
		settle = func() error {
			return receiver.backend.retry(ctx, claim.fence, failure.retry,
				artifactReceiverClampRetry(failure.retryAfter), at)
		}
	} else if failure.permanent.Valid() && failure.retry == "" {
		settle = func() error {
			return receiver.backend.quarantine(ctx, claim.fence, failure.permanent, at)
		}
	} else {
		return artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant,
			"invalid Artifact claim settlement", nil)
	}
	var settleErr error
	for probe := 0; probe < artifactReceiverResponseLossProbe; probe++ {
		settleErr = settle()
		if settleErr == nil || ctx.Err() != nil || artifactReceiverClosedStoreError(settleErr) {
			break
		}
	}
	if settleErr != nil {
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(settleErr, store.ErrPeerInboxArtifactStale) {
			receiver.recordStale()
			return nil
		}
		return receiver.storeFatal("settle Inbox Artifact failure", settleErr)
	}
	if failure.retry.Valid() {
		receiver.recordRetry()
	} else {
		receiver.recordQuarantine()
	}
	return nil
}

func (receiver *ArtifactReceiver) classifyStoreClaimFailure(operation string,
	cause error,
) (*artifactReceiverClaimFailure, error) {
	if cause == nil {
		return nil, nil
	}
	if errors.Is(cause, store.ErrPeerInboxArtifactStale) {
		receiver.recordStale()
		return nil, nil
	}
	if errors.Is(cause, store.ErrPeerInboxArtifactAuthority) {
		failure := retryArtifactReceiverClaim(store.PeerInboxArtifactRetryNotAuthorized,
			artifactReceiverRetryMinimum)
		failure.reconcile = true
		return failure, nil
	}
	if errors.Is(cause, store.ErrPeerInboxArtifactLimit) {
		return quarantineArtifactReceiverClaim(store.PeerInboxArtifactLimitExceeded), nil
	}
	return nil, receiver.storeFatal(operation, cause)
}

func classifyRemoteManifestFailure(cause error) *artifactReceiverClaimFailure {
	if errors.Is(cause, artifactdomain.ErrArtifactLimit) {
		return quarantineArtifactReceiverClaim(store.PeerInboxArtifactLimitExceeded)
	}
	return quarantineArtifactReceiverClaim(store.PeerInboxArtifactManifestInvalid)
}

func classifyImportedClosureFailure(cause error) *artifactReceiverClaimFailure {
	if errors.Is(cause, artifactdomain.ErrArtifactLimit) {
		return quarantineArtifactReceiverClaim(store.PeerInboxArtifactLimitExceeded)
	}
	if errors.Is(cause, artifactdomain.ErrClosureMismatch) {
		return quarantineArtifactReceiverClaim(store.PeerInboxArtifactDigestMismatch)
	}
	return quarantineArtifactReceiverClaim(store.PeerInboxArtifactManifestInvalid)
}

func (receiver *ArtifactReceiver) now() (time.Time, error) {
	value := receiver.clock.Now().Round(0).UTC()
	if value.IsZero() || value.Year() < 1 || value.Year() > 9999 ||
		!time.Unix(0, value.UnixNano()).UTC().Equal(value) {
		return time.Time{}, errors.New("Artifact receiver clock returned an unsupported time")
	}
	return value, nil
}

func artifactReceiverClampRetry(delay time.Duration) time.Duration {
	if delay < artifactReceiverRetryMinimum {
		return artifactReceiverRetryMinimum
	}
	if delay > artifactReceiverRetryMaximum {
		return artifactReceiverRetryMaximum
	}
	return delay
}

func artifactReceiverBackoff(attempt uint32) time.Duration {
	delay := artifactReceiverRetryMinimum
	for current := uint32(1); current < attempt && delay < artifactReceiverRetryMaximum; current++ {
		if delay > artifactReceiverRetryMaximum/2 {
			return artifactReceiverRetryMaximum
		}
		delay *= 2
	}
	return artifactReceiverClampRetry(delay)
}

func artifactReceiverResourceFailure(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) ||
		errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE)
}

func artifactReceiverClosedStoreError(err error) bool {
	return errors.Is(err, store.ErrPeerInboxArtifactInput) ||
		errors.Is(err, store.ErrPeerInboxArtifactAuthority) ||
		errors.Is(err, store.ErrPeerInboxArtifactStale) ||
		errors.Is(err, store.ErrPeerInboxArtifactNotReady) ||
		errors.Is(err, store.ErrPeerInboxArtifactLimit) ||
		errors.Is(err, store.ErrPeerInboxArtifactInvariant) ||
		errors.Is(err, store.ErrArtifactStageFence) ||
		errors.Is(err, store.ErrArtifactStageConflict) ||
		errors.Is(err, store.ErrArtifactConflict) || errors.Is(err, store.ErrArtifactUnverified) ||
		errors.Is(err, store.ErrArtifactReference)
}

func (receiver *ArtifactReceiver) storeFatal(operation string, cause error) error {
	code := ArtifactReceiverFatalStoreFailure
	if errors.Is(cause, store.ErrPeerInboxArtifactInput) ||
		errors.Is(cause, store.ErrPeerInboxArtifactInvariant) ||
		errors.Is(cause, store.ErrPeerInboxArtifactNotReady) ||
		errors.Is(cause, store.ErrArtifactStageFence) ||
		errors.Is(cause, store.ErrArtifactStageConflict) ||
		errors.Is(cause, store.ErrArtifactConflict) || errors.Is(cause, store.ErrArtifactUnverified) ||
		errors.Is(cause, store.ErrArtifactReference) {
		code = ArtifactReceiverFatalStoreInvariant
	}
	return artifactReceiverFatal(code, operation, cause)
}

func (receiver *ArtifactReceiver) casFatal(operation string, cause error) error {
	code := ArtifactReceiverFatalCASFailure
	if errors.Is(cause, artifactdomain.ErrCASInput) ||
		errors.Is(cause, artifactdomain.ErrCASCorruption) ||
		errors.Is(cause, artifactdomain.ErrClosureMismatch) || errors.Is(cause, os.ErrNotExist) {
		code = ArtifactReceiverFatalCASInvariant
	}
	return artifactReceiverFatal(code, operation, cause)
}

type artifactReceiverFatalError struct {
	code   ArtifactReceiverFatalCode
	detail string
	cause  error
}

func (failure *artifactReceiverFatalError) Error() string {
	if failure == nil {
		return ErrArtifactReceiverInvariant.Error()
	}
	if failure.cause == nil {
		return fmt.Sprintf("%s: %s", ErrArtifactReceiverInvariant, failure.detail)
	}
	return fmt.Sprintf("%s: %s: %v", ErrArtifactReceiverInvariant,
		failure.detail, failure.cause)
}

func (failure *artifactReceiverFatalError) Unwrap() error { return ErrArtifactReceiverInvariant }

func artifactReceiverFatal(code ArtifactReceiverFatalCode, detail string,
	cause error,
) error {
	return &artifactReceiverFatalError{code: code, detail: detail, cause: cause}
}

func artifactReceiverFatalCode(err error) ArtifactReceiverFatalCode {
	var failure *artifactReceiverFatalError
	if errors.As(err, &failure) && failure.code != ArtifactReceiverFatalNone {
		return failure.code
	}
	return ArtifactReceiverFatalWorkerInvariant
}

func validateArtifactReceiverClaim(claim artifactReceiverClaim, owner string, at time.Time) error {
	fence := claim.fence
	if fence.inboxID.IsZero() || fence.leaseOwner != owner || fence.attempt == 0 ||
		!fence.leaseUntil.After(at) || claim.channelID.IsZero() || claim.originPeerID.IsZero() ||
		claim.originEpoch.IsZero() || claim.publication.Event().ID().IsZero() ||
		claim.publication.Digest().IsZero() || claim.publication.WireJSON().IsZero() ||
		len(claim.requiredRoots) > artifactdomain.MaxRoots {
		return errors.New("claim has incomplete identity, publication or live fence")
	}
	scope := claim.publication.Event().Scope()
	if scope.ChannelID() != claim.channelID || scope.OriginPeerID() != claim.originPeerID ||
		scope.OriginEpoch() != claim.originEpoch {
		return errors.New("claim scope differs from publication")
	}
	artifacts := claim.publication.Event().Artifacts()
	if len(artifacts) != len(claim.requiredRoots) {
		return errors.New("claim roots differ from publication")
	}
	for index, root := range claim.requiredRoots {
		if root.IsZero() || artifacts[index].RootDigest() != root ||
			(index > 0 && claim.requiredRoots[index-1].String() >= root.String()) {
			return errors.New("claim roots are not the exact canonical publication set")
		}
	}
	return nil
}

func sameArtifactReceiverFenceGeneration(left, right artifactReceiverFence) bool {
	return left.inboxID == right.inboxID && left.leaseOwner == right.leaseOwner &&
		left.attempt == right.attempt && left.token == right.token &&
		left.hasDurable == right.hasDurable
}

func storeArtifactReceiverClosure(closure artifactdomain.Closure) store.VerifiedArtifactClosure {
	artifactRoots := closure.Roots()
	roots := make([]store.VerifiedArtifactRoot, len(artifactRoots))
	for index, root := range artifactRoots {
		roots[index] = store.VerifiedArtifactRoot{RootDigest: root.RootDigest,
			Manifest: root.Manifest, ManifestDigest: root.ManifestDigest,
			TotalBytes: root.TotalBytes, CreatedAt: root.CreatedAt, VerifiedAt: root.VerifiedAt}
	}
	artifactBlocks := closure.Blocks()
	blocks := make([]store.VerifiedArtifactBlock, len(artifactBlocks))
	for index, block := range artifactBlocks {
		blocks[index] = store.VerifiedArtifactBlock{Digest: block.Digest,
			SizeBytes: block.SizeBytes, CreatedAt: block.CreatedAt}
	}
	artifactMap := closure.BlockMap()
	rootBlocks := make([]store.VerifiedArtifactRootBlock, len(artifactMap))
	for index, mapping := range artifactMap {
		rootBlocks[index] = store.VerifiedArtifactRootBlock{RootDigest: mapping.RootDigest,
			Ordinal: mapping.Ordinal, LogicalPath: mapping.LogicalPath,
			OffsetBytes: mapping.OffsetBytes, LengthBytes: mapping.LengthBytes,
			BlockDigest: mapping.BlockDigest, Mode: mapping.Mode}
	}
	return store.VerifiedArtifactClosure{Roots: roots, Blocks: blocks, RootBlocks: rootBlocks}
}

func (backend durableArtifactReceiverBackend) claim(ctx context.Context, owner string,
	at time.Time,
) (artifactReceiverClaim, bool, error) {
	result, err := backend.store.ClaimPeerInboxArtifact(ctx,
		store.ClaimPeerInboxArtifactSpec{LeaseOwner: owner, At: at})
	if err != nil || !result.Found() {
		return artifactReceiverClaim{}, false, err
	}
	claim := result.Claim()
	return artifactReceiverClaim{fence: durableArtifactReceiverFence(claim.Fence()),
		publication: claim.Publication(), channelID: claim.ChannelID(),
		originPeerID: claim.OriginPeerID(), originEpoch: claim.OriginEpoch(),
		requiredRoots: claim.RequiredArtifactRoots()}, true, nil
}

func (backend durableArtifactReceiverBackend) renew(ctx context.Context,
	fence artifactReceiverFence, at time.Time,
) (artifactReceiverFence, error) {
	if !fence.hasDurable {
		return artifactReceiverFence{}, store.ErrPeerInboxArtifactInput
	}
	result, err := backend.store.RenewPeerInboxArtifactLease(ctx,
		store.RenewPeerInboxArtifactSpec{Fence: fence.durable, At: at})
	if err != nil {
		return artifactReceiverFence{}, err
	}
	return durableArtifactReceiverFence(result.Fence()), nil
}

func (backend durableArtifactReceiverBackend) probe(ctx context.Context,
	fence artifactReceiverFence, at time.Time,
) error {
	if !fence.hasDurable {
		return store.ErrPeerInboxArtifactInput
	}
	return backend.store.ProbePeerInboxArtifactAuthority(ctx,
		store.ProbePeerInboxArtifactAuthoritySpec{Fence: fence.durable, At: at})
}

func (backend durableArtifactReceiverBackend) readRoot(ctx context.Context,
	fence artifactReceiverFence, root model.Digest, at time.Time,
) (artifactReceiverCachedRoot, bool, error) {
	if !fence.hasDurable {
		return artifactReceiverCachedRoot{}, false, store.ErrPeerInboxArtifactInput
	}
	value, found, err := backend.store.ReadPeerInboxArtifactRoot(ctx, store.ReadPeerInboxArtifactRootSpec{
		Fence: fence.durable, RootDigest: root, At: at})
	if err != nil || !found {
		return artifactReceiverCachedRoot{}, found, err
	}
	verifiedAt, verified := value.VerifiedAt()
	return artifactReceiverCachedRoot{rootDigest: value.RootDigest(), manifest: value.Manifest(),
		manifestDigest: value.ManifestDigest(), totalBytes: value.TotalBytes(),
		createdAt: value.CreatedAt(), verifiedAt: verifiedAt, verified: verified}, true, nil
}

func (backend durableArtifactReceiverBackend) recordSource(ctx context.Context,
	fence artifactReceiverFence, source model.PeerID, at time.Time,
) error {
	if !fence.hasDurable {
		return store.ErrPeerInboxArtifactInput
	}
	_, err := backend.store.RecordPeerInboxArtifactSource(ctx,
		store.RecordPeerInboxArtifactSourceSpec{Fence: fence.durable,
			SourcePeerID: source, At: at})
	return err
}

func (backend durableArtifactReceiverBackend) ready(ctx context.Context,
	fence artifactReceiverFence, owner artifactdomain.StageOwner, at time.Time,
) error {
	if !fence.hasDurable {
		return store.ErrPeerInboxArtifactInput
	}
	result, err := backend.store.MarkPeerInboxArtifactReady(ctx,
		store.MarkPeerInboxArtifactReadySpec{
			Fence: fence.durable, Owner: owner, At: at,
		})
	if err == nil && result.Status() != model.InboxReady {
		return store.ErrPeerInboxArtifactInvariant
	}
	return err
}

func (backend durableArtifactReceiverBackend) retry(ctx context.Context,
	fence artifactReceiverFence, diagnostic store.PeerInboxArtifactRetryDiagnostic,
	after time.Duration, at time.Time,
) error {
	if !fence.hasDurable {
		return store.ErrPeerInboxArtifactInput
	}
	result, err := backend.store.RetryPeerInboxArtifact(ctx, store.RetryPeerInboxArtifactSpec{
		Fence: fence.durable, Diagnostic: diagnostic, RetryAfter: after, At: at})
	if err == nil && result.Status() != model.InboxRetry {
		return store.ErrPeerInboxArtifactInvariant
	}
	return err
}

func (backend durableArtifactReceiverBackend) quarantine(ctx context.Context,
	fence artifactReceiverFence, diagnostic store.PeerInboxArtifactPermanentDiagnostic,
	at time.Time,
) error {
	if !fence.hasDurable {
		return store.ErrPeerInboxArtifactInput
	}
	result, err := backend.store.QuarantinePeerInboxArtifact(ctx,
		store.QuarantinePeerInboxArtifactSpec{Fence: fence.durable,
			Diagnostic: diagnostic, At: at})
	if err == nil && result.Status() != model.InboxQuarantined {
		return store.ErrPeerInboxArtifactInvariant
	}
	return err
}

func durableArtifactReceiverFence(fence store.PeerInboxArtifactFence) artifactReceiverFence {
	return artifactReceiverFence{inboxID: fence.InboxID(), leaseOwner: fence.LeaseOwner(),
		leaseUntil: fence.LeaseUntil(), attempt: fence.Attempt(), durable: fence, hasDurable: true}
}

type artifactReceiverPeerGate struct {
	tokens chan struct{}
	users  int
}

type artifactReceiverPullLimiter struct {
	node      chan struct{}
	peerLimit int
	mu        sync.Mutex
	peers     map[model.PeerID]*artifactReceiverPeerGate
}

func newArtifactReceiverPullLimiter(nodeLimit, peerLimit int) (*artifactReceiverPullLimiter, error) {
	if nodeLimit <= 0 || peerLimit <= 0 || peerLimit > nodeLimit ||
		nodeLimit > HermeticLimits().NodeArtifactPulls || peerLimit > HermeticLimits().PeerArtifactPulls {
		return nil, fmt.Errorf("%w: invalid Artifact pull limits", ErrArtifactReceiver)
	}
	return &artifactReceiverPullLimiter{node: make(chan struct{}, nodeLimit), peerLimit: peerLimit,
		peers: make(map[model.PeerID]*artifactReceiverPeerGate)}, nil
}

func (limiter *artifactReceiverPullLimiter) acquire(ctx context.Context,
	peerID model.PeerID,
) (func(), error) {
	if limiter == nil || ctx == nil || peerID.IsZero() {
		return nil, ErrArtifactReceiverInvariant
	}
	limiter.mu.Lock()
	gate := limiter.peers[peerID]
	if gate == nil {
		gate = &artifactReceiverPeerGate{tokens: make(chan struct{}, limiter.peerLimit)}
		limiter.peers[peerID] = gate
	}
	gate.users++
	limiter.mu.Unlock()
	registered := true
	unregister := func() {
		if !registered {
			return
		}
		registered = false
		limiter.mu.Lock()
		gate.users--
		if gate.users == 0 {
			delete(limiter.peers, peerID)
		}
		limiter.mu.Unlock()
	}
	select {
	case gate.tokens <- struct{}{}:
	case <-ctx.Done():
		unregister()
		return nil, ctx.Err()
	}
	select {
	case limiter.node <- struct{}{}:
	case <-ctx.Done():
		<-gate.tokens
		unregister()
		return nil, ctx.Err()
	}
	return func() {
		<-limiter.node
		<-gate.tokens
		unregister()
	}, nil
}
