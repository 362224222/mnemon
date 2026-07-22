package peer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestGossipIngressPreservesRelayTransportAndOriginalAuthor(t *testing.T) {
	t.Parallel()
	fixture := newGossipIngressFixture(t, "relay")
	source := newGossipIngressTestSession(fixture.channel.ChannelID,
		fixture.local.libp2pID, fixture.received(fixture.relay.libp2pID))
	ctx, cancel := context.WithCancel(context.Background())
	durable := &gossipIngressTestStore{outcomes: []gossipIngressStoreOutcome{{
		result: store.PutPeerInboxResult{Disposition: store.PeerInboxStored},
		after:  cancel,
	}}}
	ingress := mustGossipIngress(t, source, durable, fixture.at)
	if err := ingress.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if source.NextCalls() != 1 || len(durable.Specs()) != 1 || durable.AckCalls() != 0 {
		t.Fatalf("bounded ingress calls = Next %d, Put %d, ACK %d",
			source.NextCalls(), len(durable.Specs()), durable.AckCalls())
	}
	spec := durable.Specs()[0]
	if spec.TransportPeerID != fixture.relay.modelID ||
		spec.Publication.Event().Scope().OriginPeerID() != fixture.origin.modelID ||
		spec.Publication.Event().Source() != model.EventSourceImported ||
		spec.ArrivalSource != model.ArrivalGossip || !spec.ReceivedAt.Equal(fixture.at) {
		t.Fatalf("Gossip PutPeerInbox spec lost origin/relay separation: %#v", spec)
	}
	snapshot := ingress.Snapshot()
	if snapshot.Running || snapshot.Received != 1 || snapshot.Stored != 1 ||
		snapshot.Ignored != 0 || snapshot.LastDiagnostic.Code().Valid() {
		t.Fatalf("Gossip ingress snapshot = %#v", snapshot)
	}
}

func TestGossipIngressCoversSelfAuthoredSubscriptionCopies(t *testing.T) {
	t.Parallel()
	fixture := newGossipIngressFixture(t, "self-authored")
	publication := testPeerPublication(t, fixture.channel, fixture.local, fixture.origin,
		"private-self-authored-publication")
	source := newGossipIngressTestSession(fixture.channel.ChannelID, fixture.local.libp2pID,
		ReceivedPublication{publication: publication, receivedFrom: fixture.local.libp2pID,
			originalAuthor: fixture.local.libp2pID},
		ReceivedPublication{publication: publication, receivedFrom: fixture.relay.libp2pID,
			originalAuthor: fixture.local.libp2pID},
		fixture.received(fixture.origin.libp2pID))
	ctx, cancel := context.WithCancel(context.Background())
	durable := &gossipIngressTestStore{outcomes: []gossipIngressStoreOutcome{{
		result: store.PutPeerInboxResult{Disposition: store.PeerInboxStored},
		after:  cancel,
	}}}
	ingress := mustGossipIngress(t, source, durable, fixture.at)
	if err := ingress.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if source.NextCalls() != 3 || len(durable.Specs()) != 1 {
		t.Fatalf("self-authored coverage calls = Next %d, Put %d",
			source.NextCalls(), len(durable.Specs()))
	}
	snapshot := ingress.Snapshot()
	if snapshot.Received != 3 || snapshot.Covered != 2 || snapshot.Stored != 1 ||
		snapshot.LastDiagnostic.Code().Valid() {
		t.Fatalf("self-authored coverage snapshot = %#v", snapshot)
	}
}

func TestGossipIngressRejectsPublicationTamperingBeforeStore(t *testing.T) {
	t.Parallel()
	fixture := newGossipIngressFixture(t, "tampering")
	otherChannel := testThreePeerAuthorityChannel(t, "gossip-ingress-other-channel",
		fixture.local, fixture.origin, fixture.relay)
	wrongChannel := testPeerPublication(t, otherChannel, fixture.origin, fixture.local,
		"must-never-reach-the-Store")
	tests := []struct {
		name     string
		received ReceivedPublication
	}{
		{name: "local callback", received: fixture.received(fixture.origin.libp2pID)},
		{name: "self transport", received: fixture.received(fixture.local.libp2pID)},
		{name: "wrong original author", received: ReceivedPublication{publication: fixture.publication,
			receivedFrom: fixture.relay.libp2pID, originalAuthor: fixture.relay.libp2pID}},
		{name: "self original author", received: ReceivedPublication{publication: fixture.publication,
			receivedFrom: fixture.relay.libp2pID, originalAuthor: fixture.local.libp2pID}},
		{name: "cross Channel", received: ReceivedPublication{publication: wrongChannel,
			receivedFrom: fixture.relay.libp2pID, originalAuthor: fixture.origin.libp2pID}},
	}
	// Mark only the first case as a local PubSub callback after constructing it
	// with otherwise-valid direct-origin metadata.
	tests[0].received.local = true

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := newGossipIngressTestSession(fixture.channel.ChannelID,
				fixture.local.libp2pID, test.received)
			durable := &gossipIngressTestStore{}
			ingress := mustGossipIngress(t, source, durable, fixture.at)
			err := ingress.Run(context.Background())
			assertGossipIngressFailure(t, err, GossipIngressDiagnosticPublication, false)
			if len(durable.Specs()) != 0 || durable.AckCalls() != 0 {
				t.Fatal("tampered publication reached a durable Store or ACK path")
			}
			if strings.Contains(err.Error(), fixture.publication.Event().Summary()) ||
				strings.Contains(err.Error(), fixture.origin.modelID.String()) {
				t.Fatalf("diagnostic leaked publication or Peer data: %q", err)
			}
		})
	}
}

func TestGossipIngressPressurePausesWithoutAckUntilExplicitRetry(t *testing.T) {
	t.Parallel()
	fixture := newGossipIngressFixture(t, "pressure")
	source := newGossipIngressTestSession(fixture.channel.ChannelID, fixture.local.libp2pID,
		fixture.received(fixture.origin.libp2pID), fixture.received(fixture.relay.libp2pID))
	secondContext, cancelSecond := context.WithCancel(context.Background())
	durable := &gossipIngressTestStore{outcomes: []gossipIngressStoreOutcome{
		{err: store.ErrPeerInboxPressure},
		{result: store.PutPeerInboxResult{Disposition: store.PeerInboxDuplicate}, after: cancelSecond},
	}}
	ingress := mustGossipIngress(t, source, durable, fixture.at)
	err := ingress.Run(context.Background())
	assertGossipIngressFailure(t, err, GossipIngressDiagnosticPressure, true)
	if source.NextCalls() != 1 || len(durable.Specs()) != 1 || durable.AckCalls() != 0 {
		t.Fatalf("pressure consumed or acknowledged later live data: Next %d, Put %d, ACK %d",
			source.NextCalls(), len(durable.Specs()), durable.AckCalls())
	}
	var failure *GossipIngressFailure
	if !errors.As(err, &failure) || failure.RetryAfter() != gossipIngressFastRetry {
		t.Fatalf("pressure retry policy = %#v", failure)
	}

	// The owner explicitly chooses when to resume this bounded session. Until
	// then no later publication is consumed; a missed live copy remains Pull
	// repair work rather than a fabricated Gossip ACK.
	if err := ingress.Run(secondContext); err != nil {
		t.Fatal(err)
	}
	if source.NextCalls() != 2 || len(durable.Specs()) != 2 || durable.AckCalls() != 0 {
		t.Fatalf("explicit retry = Next %d, Put %d, ACK %d",
			source.NextCalls(), len(durable.Specs()), durable.AckCalls())
	}
	snapshot := ingress.Snapshot()
	if snapshot.Duplicates != 1 || snapshot.LastDiagnostic.Code() != GossipIngressDiagnosticPressure {
		t.Fatalf("pressure/retry snapshot = %#v", snapshot)
	}
}

func TestGossipIngressSignalsDurableRepairForGapAndPressure(t *testing.T) {
	t.Parallel()
	fixture := newGossipIngressFixture(t, "repair-signal")

	t.Run("durable cursor advanced", func(t *testing.T) {
		assertGossipIngressDurableRepairSignal(t, fixture, "contiguous repair signal", 3, 3)
	})

	t.Run("durable cursor gap", func(t *testing.T) {
		assertGossipIngressDurableRepairSignal(t, fixture, "gap repair signal", 1, 3)
	})

	t.Run("pressure", func(t *testing.T) {
		source := newGossipIngressTestSession(fixture.channel.ChannelID, fixture.local.libp2pID,
			fixture.received(fixture.relay.libp2pID))
		trigger := &gossipIngressTestRepairTrigger{}
		durable := &gossipIngressTestStore{outcomes: []gossipIngressStoreOutcome{{
			err: store.ErrPeerInboxPressure,
		}}}
		ingress, err := newGossipIngress(source, durable,
			fixedGossipIngressClock{at: fixture.at}, trigger)
		if err != nil {
			t.Fatal(err)
		}
		err = ingress.Run(context.Background())
		assertGossipIngressFailure(t, err, GossipIngressDiagnosticPressure, true)
		if trigger.Calls() != 1 || durable.AckCalls() != 0 {
			t.Fatalf("pressure repair signal = %d, ACK %d", trigger.Calls(), durable.AckCalls())
		}
	})
}

func assertGossipIngressDurableRepairSignal(t *testing.T, fixture gossipIngressFixture,
	label string, contiguous, observed uint64,
) {
	t.Helper()
	source := newGossipIngressTestSession(fixture.channel.ChannelID, fixture.local.libp2pID,
		fixture.received(fixture.origin.libp2pID))
	ctx, cancel := context.WithCancel(context.Background())
	trigger := &gossipIngressTestRepairTrigger{}
	durable := &gossipIngressTestStore{outcomes: []gossipIngressStoreOutcome{{
		result: store.PutPeerInboxResult{Disposition: store.PeerInboxStored,
			Cursor: store.PeerCursorProjection{ContiguousChannelSequence: contiguous,
				ObservedChannelSequence: observed}}, after: cancel,
	}}}
	ingress, err := newGossipIngress(source, durable,
		fixedGossipIngressClock{at: fixture.at}, trigger)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.Run(ctx); err != nil || trigger.Calls() != 1 || durable.AckCalls() != 0 {
		t.Fatalf("%s = Run %v, signals %d, ACK %d",
			label, err, trigger.Calls(), durable.AckCalls())
	}
}

func TestGossipIngressQuarantineAndDurableConflictDoNotStarveOtherOrigins(t *testing.T) {
	t.Parallel()
	fixture := newGossipIngressFixture(t, "origin-isolation")
	source := newGossipIngressTestSession(fixture.channel.ChannelID, fixture.local.libp2pID,
		fixture.received(fixture.origin.libp2pID), fixture.received(fixture.relay.libp2pID),
		fixture.received(fixture.origin.libp2pID))
	ctx, cancel := context.WithCancel(context.Background())
	durable := &gossipIngressTestStore{outcomes: []gossipIngressStoreOutcome{
		{err: store.ErrPeerInboxQuarantined},
		{result: store.PutPeerInboxResult{Disposition: store.PeerInboxConflicted}},
		{result: store.PutPeerInboxResult{Disposition: store.PeerInboxIgnored}, after: cancel},
	}}
	ingress := mustGossipIngress(t, source, durable, fixture.at)
	if err := ingress.Run(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := ingress.Snapshot()
	if source.NextCalls() != 3 || len(durable.Specs()) != 3 || snapshot.Received != 3 ||
		snapshot.Quarantined != 1 || snapshot.Conflicted != 1 || snapshot.Ignored != 1 ||
		snapshot.LastDiagnostic.Code() != GossipIngressDiagnosticConflict ||
		snapshot.LastDiagnostic.Retryable() || durable.AckCalls() != 0 {
		t.Fatalf("quarantine/conflict isolation = snapshot %#v, Next %d, Put %d, ACK %d",
			snapshot, source.NextCalls(), len(durable.Specs()), durable.AckCalls())
	}
}

func TestGossipIngressUsesClosedRedactedStopPolicy(t *testing.T) {
	t.Parallel()
	fixture := newGossipIngressFixture(t, "diagnostics")
	private := errors.New("sqlite /private/path included secret publication bytes")
	tests := []struct {
		name      string
		storeErr  error
		code      GossipIngressDiagnosticCode
		retryable bool
		retry     time.Duration
	}{
		{name: "authority", storeErr: fmtWrapped(store.ErrPeerInboxAuthority, private),
			code: GossipIngressDiagnosticAuthority, retryable: true, retry: gossipIngressFastRetry},
		{name: "conflict invariant", storeErr: fmtWrapped(store.ErrPeerInboxConflict, private),
			code: GossipIngressDiagnosticConflict},
		{name: "input invariant", storeErr: fmtWrapped(store.ErrPeerInboxInput, private),
			code: GossipIngressDiagnosticPublication},
		{name: "unknown Store", storeErr: private,
			code: GossipIngressDiagnosticStore, retryable: true, retry: gossipIngressStoreRetry},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := newGossipIngressTestSession(fixture.channel.ChannelID,
				fixture.local.libp2pID, fixture.received(fixture.relay.libp2pID))
			durable := &gossipIngressTestStore{outcomes: []gossipIngressStoreOutcome{{err: test.storeErr}}}
			ingress := mustGossipIngress(t, source, durable, fixture.at)
			err := ingress.Run(context.Background())
			assertGossipIngressFailure(t, err, test.code, test.retryable)
			var failure *GossipIngressFailure
			if !errors.As(err, &failure) || failure.RetryAfter() != test.retry ||
				strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "secret") {
				t.Fatalf("closed diagnostic = %v / %#v", err, failure)
			}
		})
	}
}

func TestGossipIngressStopsCleanlyOnCancellationAndSessionRetirement(t *testing.T) {
	t.Parallel()
	fixture := newGossipIngressFixture(t, "lifecycle")

	cancelledSource := newGossipIngressTestSession(fixture.channel.ChannelID,
		fixture.local.libp2pID, fixture.received(fixture.origin.libp2pID))
	cancelledStore := &gossipIngressTestStore{}
	cancelled := mustGossipIngress(t, cancelledSource, cancelledStore, fixture.at)
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cancelled.Run(cancelledContext); err != nil || cancelledSource.NextCalls() != 0 ||
		len(cancelledStore.Specs()) != 0 {
		t.Fatalf("pre-cancelled Run = %v, Next %d, Put %d", err,
			cancelledSource.NextCalls(), len(cancelledStore.Specs()))
	}

	retiredSource := newGossipIngressTestSession(fixture.channel.ChannelID,
		fixture.local.libp2pID, fixture.received(fixture.relay.libp2pID))
	retiredSource.retireOnNext = true
	retiredStore := &gossipIngressTestStore{}
	retired := mustGossipIngress(t, retiredSource, retiredStore, fixture.at)
	if err := retired.Run(context.Background()); err != nil || retiredSource.NextCalls() != 1 ||
		len(retiredStore.Specs()) != 0 || retired.Snapshot().Running {
		t.Fatalf("retired generation Run = %v, Next %d, Put %d, snapshot %#v", err,
			retiredSource.NextCalls(), len(retiredStore.Specs()), retired.Snapshot())
	}

	blockedSource := newGossipIngressTestSession(fixture.channel.ChannelID, fixture.local.libp2pID)
	blockedStore := &gossipIngressTestStore{}
	blocked := mustGossipIngress(t, blockedSource, blockedStore, fixture.at)
	done := make(chan error, 1)
	go func() { done <- blocked.Run(context.Background()) }()
	blockedSource.WaitNext(t)
	blockedSource.Retire()
	select {
	case err := <-done:
		if err != nil || len(blockedStore.Specs()) != 0 {
			t.Fatalf("blocked retired Run = %v, Put %d", err, len(blockedStore.Specs()))
		}
	case <-time.After(time.Second):
		t.Fatal("retired TopicSession did not stop the ingress loop")
	}
}

func TestGossipIngressRejectsConcurrentRunWithoutConsuming(t *testing.T) {
	t.Parallel()
	fixture := newGossipIngressFixture(t, "concurrent-run")
	source := newGossipIngressTestSession(fixture.channel.ChannelID, fixture.local.libp2pID)
	durable := &gossipIngressTestStore{}
	ingress := mustGossipIngress(t, source, durable, fixture.at)

	firstDone := make(chan error, 1)
	go func() { firstDone <- ingress.Run(context.Background()) }()
	source.WaitNext(t)
	secondErr := ingress.Run(context.Background())
	assertGossipIngressFailure(t, secondErr, GossipIngressDiagnosticSession, true)
	if source.NextCalls() != 1 || len(durable.Specs()) != 0 || !ingress.Snapshot().Running {
		t.Fatalf("concurrent Run consumed data: Next %d, Put %d, snapshot %#v",
			source.NextCalls(), len(durable.Specs()), ingress.Snapshot())
	}

	source.Retire()
	select {
	case err := <-firstDone:
		if err != nil || ingress.Snapshot().Running {
			t.Fatalf("retired first Run = %v, snapshot %#v", err, ingress.Snapshot())
		}
	case <-time.After(time.Second):
		t.Fatal("first Gossip ingress Run did not retire")
	}
}

type gossipIngressFixture struct {
	local       authorityTestPeer
	origin      authorityTestPeer
	relay       authorityTestPeer
	channel     ChannelAuthoritySnapshot
	publication model.SignedPublication
	at          time.Time
}

func newGossipIngressFixture(t *testing.T, seed string) gossipIngressFixture {
	t.Helper()
	local := testAuthorityPeer(t, "gossip-ingress-"+seed+"-local")
	origin := testAuthorityPeer(t, "gossip-ingress-"+seed+"-origin")
	relay := testAuthorityPeer(t, "gossip-ingress-"+seed+"-relay")
	channel := testThreePeerAuthorityChannel(t, "gossip-ingress-"+seed+"-channel",
		local, origin, relay)
	publication := testPeerPublication(t, channel, origin, local,
		"private-gossip-ingress-"+seed+"-publication")
	return gossipIngressFixture{local: local, origin: origin, relay: relay,
		channel: channel, publication: publication,
		at: time.Date(2026, 7, 19, 1, 2, 3, 4, time.UTC)}
}

func (fixture gossipIngressFixture) received(transport libp2ppeer.ID) ReceivedPublication {
	return ReceivedPublication{publication: fixture.publication, receivedFrom: transport,
		originalAuthor: fixture.origin.libp2pID}
}

type fixedGossipIngressClock struct{ at time.Time }

func (clock fixedGossipIngressClock) Now() time.Time { return clock.at }

type gossipIngressTestSession struct {
	mu           sync.Mutex
	channelID    model.ChannelID
	localPeerID  libp2ppeer.ID
	current      bool
	items        []ReceivedPublication
	nextCalls    int
	nextEntered  chan struct{}
	retired      chan struct{}
	retireOnce   sync.Once
	retireOnNext bool
}

func newGossipIngressTestSession(channelID model.ChannelID, local libp2ppeer.ID,
	items ...ReceivedPublication,
) *gossipIngressTestSession {
	return &gossipIngressTestSession{channelID: channelID, localPeerID: local, current: true,
		items: append([]ReceivedPublication(nil), items...), nextEntered: make(chan struct{}),
		retired: make(chan struct{})}
}

func (session *gossipIngressTestSession) ChannelID() model.ChannelID { return session.channelID }

func (session *gossipIngressTestSession) gossipIngressLocalPeerID() libp2ppeer.ID {
	return session.localPeerID
}

func (session *gossipIngressTestSession) IsCurrent() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.current
}

func (session *gossipIngressTestSession) Next(ctx context.Context) (ReceivedPublication, error) {
	session.mu.Lock()
	session.nextCalls++
	session.retireOnce.Do(func() { close(session.nextEntered) })
	if len(session.items) > 0 {
		item := session.items[0]
		session.items = session.items[1:]
		if session.retireOnNext {
			session.current = false
		}
		session.mu.Unlock()
		return item, nil
	}
	retired := session.retired
	session.mu.Unlock()
	select {
	case <-ctx.Done():
		return ReceivedPublication{}, ctx.Err()
	case <-retired:
		return ReceivedPublication{}, ErrGossipTopic
	}
}

func (session *gossipIngressTestSession) NextCalls() int {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.nextCalls
}

func (session *gossipIngressTestSession) WaitNext(t *testing.T) {
	t.Helper()
	select {
	case <-session.nextEntered:
	case <-time.After(time.Second):
		t.Fatal("Gossip ingress did not enter Next")
	}
}

func (session *gossipIngressTestSession) Retire() {
	session.mu.Lock()
	session.current = false
	session.mu.Unlock()
	session.retireOnce.Do(func() { close(session.nextEntered) })
	select {
	case <-session.retired:
	default:
		close(session.retired)
	}
}

type gossipIngressStoreOutcome struct {
	result store.PutPeerInboxResult
	err    error
	after  func()
}

type gossipIngressTestStore struct {
	mu       sync.Mutex
	outcomes []gossipIngressStoreOutcome
	specs    []store.PutPeerInboxSpec
	ackCalls int
}

type gossipIngressTestRepairTrigger struct {
	mu    sync.Mutex
	calls int
}

func (trigger *gossipIngressTestRepairTrigger) Trigger() {
	trigger.mu.Lock()
	trigger.calls++
	trigger.mu.Unlock()
}

func (trigger *gossipIngressTestRepairTrigger) Calls() int {
	trigger.mu.Lock()
	defer trigger.mu.Unlock()
	return trigger.calls
}

func (durable *gossipIngressTestStore) PutPeerInbox(_ context.Context,
	spec store.PutPeerInboxSpec,
) (store.PutPeerInboxResult, error) {
	durable.mu.Lock()
	durable.specs = append(durable.specs, spec)
	var outcome gossipIngressStoreOutcome
	if len(durable.outcomes) > 0 {
		outcome = durable.outcomes[0]
		durable.outcomes = durable.outcomes[1:]
	}
	durable.mu.Unlock()
	if outcome.after != nil {
		outcome.after()
	}
	return outcome.result, outcome.err
}

// Acknowledge deliberately sits outside GossipIngressStore. If the worker ever
// grows a false Gossip ACK dependency, this oracle catches the behavior.
func (durable *gossipIngressTestStore) Acknowledge() {
	durable.mu.Lock()
	durable.ackCalls++
	durable.mu.Unlock()
}

func (durable *gossipIngressTestStore) Specs() []store.PutPeerInboxSpec {
	durable.mu.Lock()
	defer durable.mu.Unlock()
	return append([]store.PutPeerInboxSpec(nil), durable.specs...)
}

func (durable *gossipIngressTestStore) AckCalls() int {
	durable.mu.Lock()
	defer durable.mu.Unlock()
	return durable.ackCalls
}

func mustGossipIngress(t *testing.T, session gossipIngressSession,
	durable GossipIngressStore, at time.Time,
) *GossipIngress {
	t.Helper()
	ingress, err := newGossipIngress(session, durable, fixedGossipIngressClock{at: at})
	if err != nil {
		t.Fatal(err)
	}
	return ingress
}

func assertGossipIngressFailure(t *testing.T, err error,
	code GossipIngressDiagnosticCode, retryable bool,
) {
	t.Helper()
	var failure *GossipIngressFailure
	if !errors.Is(err, ErrGossipIngress) || !errors.As(err, &failure) ||
		failure.Code() != code || failure.Retryable() != retryable ||
		failure.RetryAfter() != newGossipIngressDiagnostic(code).RetryAfter() {
		t.Fatalf("Gossip ingress failure = %v / %#v, want %s retryable=%v",
			err, failure, code, retryable)
	}
}

func fmtWrapped(sentinel, detail error) error {
	return &gossipIngressWrappedError{sentinel: sentinel, detail: detail}
}

type gossipIngressWrappedError struct{ sentinel, detail error }

func (wrapped *gossipIngressWrappedError) Error() string {
	return wrapped.sentinel.Error() + ": " + wrapped.detail.Error()
}
func (wrapped *gossipIngressWrappedError) Unwrap() error { return wrapped.sentinel }
