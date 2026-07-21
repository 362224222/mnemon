package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrGossipIngress = errors.New("Mnemon Gossip ingress")

const (
	gossipIngressFastRetry  = 250 * time.Millisecond
	gossipIngressStoreRetry = time.Second
)

// GossipIngressDiagnosticCode is a closed operational result. It deliberately
// carries no Store error, publication body or transport address: callers can
// make retry/readiness decisions without leaking untrusted or durable bytes.
type GossipIngressDiagnosticCode string

const (
	GossipIngressDiagnosticPressure    GossipIngressDiagnosticCode = "pressure"
	GossipIngressDiagnosticAuthority   GossipIngressDiagnosticCode = "authority"
	GossipIngressDiagnosticQuarantine  GossipIngressDiagnosticCode = "quarantine"
	GossipIngressDiagnosticConflict    GossipIngressDiagnosticCode = "conflict"
	GossipIngressDiagnosticPublication GossipIngressDiagnosticCode = "invalid_publication"
	GossipIngressDiagnosticSession     GossipIngressDiagnosticCode = "session_unavailable"
	GossipIngressDiagnosticStore       GossipIngressDiagnosticCode = "store_unavailable"
)

func (code GossipIngressDiagnosticCode) Valid() bool {
	switch code {
	case GossipIngressDiagnosticPressure, GossipIngressDiagnosticAuthority,
		GossipIngressDiagnosticQuarantine, GossipIngressDiagnosticConflict,
		GossipIngressDiagnosticPublication, GossipIngressDiagnosticSession,
		GossipIngressDiagnosticStore:
		return true
	default:
		return false
	}
}

type GossipIngressDiagnostic struct {
	code       GossipIngressDiagnosticCode
	retryable  bool
	retryAfter time.Duration
}

func (diagnostic GossipIngressDiagnostic) Code() GossipIngressDiagnosticCode {
	return diagnostic.code
}
func (diagnostic GossipIngressDiagnostic) Retryable() bool { return diagnostic.retryable }
func (diagnostic GossipIngressDiagnostic) RetryAfter() time.Duration {
	return diagnostic.retryAfter
}

// GossipIngressFailure is returned only when the serial receive loop must be
// restarted or retired. Quarantine and a durably recorded conflict are exposed
// through Snapshot and do not stop unrelated origins on the same Channel.
type GossipIngressFailure struct{ diagnostic GossipIngressDiagnostic }

func (failure *GossipIngressFailure) Error() string {
	if failure == nil {
		return ErrGossipIngress.Error()
	}
	return fmt.Sprintf("%s: %s", ErrGossipIngress, failure.diagnostic.code)
}

func (failure *GossipIngressFailure) Unwrap() error { return ErrGossipIngress }

func (failure *GossipIngressFailure) Code() GossipIngressDiagnosticCode {
	if failure == nil {
		return ""
	}
	return failure.diagnostic.Code()
}
func (failure *GossipIngressFailure) Retryable() bool {
	return failure != nil && failure.diagnostic.Retryable()
}
func (failure *GossipIngressFailure) RetryAfter() time.Duration {
	if failure == nil {
		return 0
	}
	return failure.diagnostic.RetryAfter()
}

type GossipIngressStore interface {
	PutPeerInbox(context.Context, store.PutPeerInboxSpec) (store.PutPeerInboxResult, error)
}

type GossipIngressClock interface{ Now() time.Time }

// GossipRepairTrigger is the intentionally tiny bridge from best-effort live
// delivery to durable origin repair. Trigger must be non-blocking and
// coalescing; Gossip ingress never waits for, or acknowledges, repair work.
type GossipRepairTrigger interface{ Trigger() }

// GossipInboxTrigger wakes durable Artifact and semantic consumers after an
// Inbox put. It is only a latency hint; startup and periodic scans remain the
// restart authority.
type GossipInboxTrigger interface{ Trigger() }

type wallGossipIngressClock struct{}

func (wallGossipIngressClock) Now() time.Time { return time.Now() }

type GossipIngressOptions struct {
	Session       *TopicSession
	Store         GossipIngressStore
	Clock         GossipIngressClock
	RepairTrigger GossipRepairTrigger
}

type GossipIngressSnapshot struct {
	Running        bool
	Received       uint64
	Stored         uint64
	Ignored        uint64
	Duplicates     uint64
	Covered        uint64
	Conflicted     uint64
	Quarantined    uint64
	LastDiagnostic GossipIngressDiagnostic
}

type gossipIngressSession interface {
	ChannelID() model.ChannelID
	IsCurrent() bool
	Next(context.Context) (ReceivedPublication, error)
	gossipIngressLocalPeerID() libp2ppeer.ID
}

type gossipIngressAdmission uint8

const (
	gossipIngressAdmissionInvalid gossipIngressAdmission = iota
	gossipIngressAdmissionCovered
	gossipIngressAdmissionImported
)

// GossipIngress is one synchronous, bounded admission loop for one current
// TopicSession. It owns no queue and starts no goroutine: at most one validated
// live publication can be inside PutPeerInbox, while subscription overflow is
// repaired later from the signed origin log.
type GossipIngress struct {
	session gossipIngressSession
	store   GossipIngressStore
	clock   GossipIngressClock
	repair  GossipRepairTrigger
	inbox   GossipInboxTrigger

	mu       sync.Mutex
	snapshot GossipIngressSnapshot
}

func NewGossipIngress(options GossipIngressOptions) (*GossipIngress, error) {
	if options.Session == nil {
		return nil, fmt.Errorf("%w: TopicSession is required", ErrGossipIngress)
	}
	return newGossipIngress(options.Session, options.Store, options.Clock, options.RepairTrigger)
}

func newGossipIngress(session gossipIngressSession, durable GossipIngressStore,
	clock GossipIngressClock, repair ...GossipRepairTrigger,
) (*GossipIngress, error) {
	if session == nil || durable == nil || session.ChannelID().IsZero() ||
		session.gossipIngressLocalPeerID() == "" {
		return nil, fmt.Errorf("%w: session, Store and local identity are required", ErrGossipIngress)
	}
	if clock == nil {
		clock = wallGossipIngressClock{}
	}
	var trigger GossipRepairTrigger
	if len(repair) > 1 {
		return nil, fmt.Errorf("%w: at most one repair trigger is allowed", ErrGossipIngress)
	}
	if len(repair) == 1 {
		trigger = repair[0]
	}
	return &GossipIngress{session: session, store: durable, clock: clock, repair: trigger}, nil
}

func (session *TopicSession) gossipIngressLocalPeerID() libp2ppeer.ID {
	if session == nil || session.gossip == nil || session.gossip.authority == nil {
		return ""
	}
	return session.gossip.authority.LocalPeerID()
}

func (ingress *GossipIngress) Snapshot() GossipIngressSnapshot {
	if ingress == nil {
		return GossipIngressSnapshot{}
	}
	ingress.mu.Lock()
	defer ingress.mu.Unlock()
	return ingress.snapshot
}

// Run drains exactly one TopicSession generation. Context cancellation and a
// retired generation are clean lifecycle exits. Pressure stops consumption so
// later live copies remain explicitly unclaimed and are recovered by Pull;
// there is no Gossip ACK path in this worker or its narrow Store surface.
func (ingress *GossipIngress) Run(ctx context.Context) error {
	if ingress == nil || ingress.session == nil || ingress.store == nil || ctx == nil {
		return newGossipIngressFailure(GossipIngressDiagnosticPublication)
	}
	if ctx.Err() != nil || !ingress.session.IsCurrent() {
		return nil
	}
	if !ingress.beginRun() {
		return newGossipIngressFailure(GossipIngressDiagnosticSession)
	}
	defer ingress.finishRun()

	for {
		if ctx.Err() != nil || !ingress.session.IsCurrent() {
			return nil
		}
		received, err := ingress.session.Next(ctx)
		if err != nil {
			if ctx.Err() != nil || !ingress.session.IsCurrent() {
				return nil
			}
			ingress.signalRepair()
			return ingress.stop(GossipIngressDiagnosticSession)
		}
		ingress.recordReceived()
		// Reconciliation may retire the generation after Next unblocks. Never
		// let a formerly authorized callback cross that lifecycle boundary.
		if ctx.Err() != nil || !ingress.session.IsCurrent() {
			return nil
		}
		if err := ingress.putReceived(ctx, received); err != nil {
			return err
		}
	}
}

func (ingress *GossipIngress) putReceived(ctx context.Context, received ReceivedPublication) error {
	spec, imported, err := ingress.admitForStore(received)
	if err != nil || !imported {
		return err
	}
	result, err := ingress.store.PutPeerInbox(ctx, spec)
	if err != nil {
		return ingress.handleStoreError(ctx, err)
	}
	if !ingress.recordDisposition(result.Disposition) {
		return ingress.stop(GossipIngressDiagnosticStore)
	}
	ingress.signalInbox()
	if result.Disposition != store.PeerInboxConflicted &&
		result.Cursor.ObservedChannelSequence > result.Cursor.ContiguousChannelSequence {
		ingress.signalRepair()
	}
	return nil
}

func (ingress *GossipIngress) handleStoreError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	switch {
	case errors.Is(err, store.ErrPeerInboxQuarantined):
		ingress.recordQuarantine()
		return nil
	case errors.Is(err, store.ErrPeerInboxPressure):
		ingress.signalRepair()
		return ingress.stop(GossipIngressDiagnosticPressure)
	case errors.Is(err, store.ErrPeerInboxAuthority):
		return ingress.stop(GossipIngressDiagnosticAuthority)
	case errors.Is(err, store.ErrPeerInboxConflict):
		return ingress.stop(GossipIngressDiagnosticConflict)
	case errors.Is(err, store.ErrPeerInboxInput):
		return ingress.stop(GossipIngressDiagnosticPublication)
	default:
		return ingress.stop(GossipIngressDiagnosticStore)
	}
}

func (ingress *GossipIngress) admitForStore(received ReceivedPublication) (
	store.PutPeerInboxSpec, bool, error,
) {
	spec, admission := ingress.admit(received)
	switch admission {
	case gossipIngressAdmissionCovered:
		// PubSub delivers the locally signed publish to its own subscription.
		// The durable origin log already covers that exact publication, so it
		// must not enter the imported Inbox or stop the Channel receive loop.
		if !ingress.recordDisposition(store.PeerInboxCovered) {
			return store.PutPeerInboxSpec{}, false,
				ingress.stop(GossipIngressDiagnosticStore)
		}
		return store.PutPeerInboxSpec{}, false, nil
	case gossipIngressAdmissionImported:
		return spec, true, nil
	default:
		return store.PutPeerInboxSpec{}, false,
			ingress.stop(GossipIngressDiagnosticPublication)
	}
}

func (ingress *GossipIngress) signalInbox() {
	if ingress != nil && ingress.inbox != nil {
		ingress.inbox.Trigger()
	}
}

func (ingress *GossipIngress) signalRepair() {
	if ingress != nil && ingress.repair != nil {
		ingress.repair.Trigger()
	}
}

func (ingress *GossipIngress) admit(received ReceivedPublication) (
	store.PutPeerInboxSpec, gossipIngressAdmission,
) {
	publication := received.Publication()
	projected, err := model.ProjectImportedPublication(&publication)
	if err != nil {
		return store.PutPeerInboxSpec{}, gossipIngressAdmissionInvalid
	}
	local := ingress.session.gossipIngressLocalPeerID()
	transport, author := received.ReceivedFrom(), received.OriginalAuthor()
	scope := projected.Event().Scope()
	if local == "" || transport == "" || author == "" ||
		scope.ChannelID() != ingress.session.ChannelID() ||
		author.String() != scope.OriginPeerID().String() {
		return store.PutPeerInboxSpec{}, gossipIngressAdmissionInvalid
	}
	transportID, transportErr := model.ParsePeerID(transport.String())
	authorID, authorErr := model.ParsePeerID(author.String())
	localID, localErr := model.ParsePeerID(local.String())
	if transportErr != nil || authorErr != nil || localErr != nil ||
		authorID != scope.OriginPeerID() {
		return store.PutPeerInboxSpec{}, gossipIngressAdmissionInvalid
	}
	if authorID == localID {
		if received.Local() && transportID != localID {
			return store.PutPeerInboxSpec{}, gossipIngressAdmissionInvalid
		}
		return store.PutPeerInboxSpec{}, gossipIngressAdmissionCovered
	}
	if received.Local() || transportID == localID {
		return store.PutPeerInboxSpec{}, gossipIngressAdmissionInvalid
	}
	receivedAt := ingress.clock.Now().Round(0).UTC()
	if receivedAt.IsZero() {
		return store.PutPeerInboxSpec{}, gossipIngressAdmissionInvalid
	}
	return store.PutPeerInboxSpec{Publication: projected, TransportPeerID: transportID,
		ArrivalSource: model.ArrivalGossip, ReceivedAt: receivedAt}, gossipIngressAdmissionImported
}

func newGossipIngressDiagnostic(code GossipIngressDiagnosticCode) GossipIngressDiagnostic {
	switch code {
	case GossipIngressDiagnosticPressure, GossipIngressDiagnosticAuthority,
		GossipIngressDiagnosticSession:
		return GossipIngressDiagnostic{code: code, retryable: true,
			retryAfter: gossipIngressFastRetry}
	case GossipIngressDiagnosticStore:
		return GossipIngressDiagnostic{code: code, retryable: true,
			retryAfter: gossipIngressStoreRetry}
	case GossipIngressDiagnosticQuarantine, GossipIngressDiagnosticConflict,
		GossipIngressDiagnosticPublication:
		return GossipIngressDiagnostic{code: code}
	default:
		return GossipIngressDiagnostic{code: GossipIngressDiagnosticStore,
			retryable: true, retryAfter: gossipIngressStoreRetry}
	}
}

func newGossipIngressFailure(code GossipIngressDiagnosticCode) *GossipIngressFailure {
	return &GossipIngressFailure{diagnostic: newGossipIngressDiagnostic(code)}
}

func (ingress *GossipIngress) beginRun() bool {
	ingress.mu.Lock()
	defer ingress.mu.Unlock()
	if ingress.snapshot.Running {
		return false
	}
	ingress.snapshot.Running = true
	return true
}

func (ingress *GossipIngress) finishRun() {
	ingress.mu.Lock()
	ingress.snapshot.Running = false
	ingress.mu.Unlock()
}

func (ingress *GossipIngress) stop(code GossipIngressDiagnosticCode) error {
	diagnostic := newGossipIngressDiagnostic(code)
	ingress.mu.Lock()
	ingress.snapshot.LastDiagnostic = diagnostic
	ingress.mu.Unlock()
	return &GossipIngressFailure{diagnostic: diagnostic}
}

func (ingress *GossipIngress) recordReceived() {
	ingress.mu.Lock()
	ingress.snapshot.Received++
	ingress.mu.Unlock()
}

func (ingress *GossipIngress) recordQuarantine() {
	diagnostic := newGossipIngressDiagnostic(GossipIngressDiagnosticQuarantine)
	ingress.mu.Lock()
	ingress.snapshot.Quarantined++
	ingress.snapshot.LastDiagnostic = diagnostic
	ingress.mu.Unlock()
}

func (ingress *GossipIngress) recordDisposition(disposition store.PeerInboxDisposition) bool {
	ingress.mu.Lock()
	defer ingress.mu.Unlock()
	switch disposition {
	case store.PeerInboxStored:
		ingress.snapshot.Stored++
	case store.PeerInboxIgnored:
		ingress.snapshot.Ignored++
	case store.PeerInboxDuplicate:
		ingress.snapshot.Duplicates++
	case store.PeerInboxCovered:
		ingress.snapshot.Covered++
	case store.PeerInboxConflicted:
		ingress.snapshot.Conflicted++
		ingress.snapshot.LastDiagnostic = newGossipIngressDiagnostic(GossipIngressDiagnosticConflict)
	default:
		return false
	}
	return true
}
