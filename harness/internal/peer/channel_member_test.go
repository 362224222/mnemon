package peer

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelMemberServiceBootstrapsUnknownAndCommitsBaselineBeforeAck(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-bootstrap")
	service := fixture.service(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dispatcher := fixture.serve(t, ctx, service)
	defer dispatcher.Close()

	remoteLibp2pID, err := canonicalLibp2pID(fixture.remote.Identity().PeerID())
	if err != nil {
		t.Fatal(err)
	}
	if fixture.controller.runtime.CanConnect(remoteLibp2pID) {
		t.Fatal("unknown peer had runtime authority before owner-signed proof")
	}
	hello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: fixture.channel.Roster().Head(),
		OwnerSignedProofChain: fixture.channel.Roster().Members()})
	if err != nil {
		t.Fatal(err)
	}
	helloResponse := fixture.roundTrip(t, ctx, 0x11, hello)
	helloAck, ok := helloResponse.Payload().(MemberHelloAck)
	if helloResponse.Type() != ChannelFrameMemberHelloAck || !ok ||
		helloAck.RosterHead() != fixture.channel.Roster().Head() || len(helloAck.MissingRecords()) != 0 {
		t.Fatalf("unknown proof acknowledgement = %#v", helloResponse)
	}
	if !fixture.controller.helloCommitted.Load() || !fixture.controller.runtimeInstalled.Load() ||
		!fixture.controller.runtime.CanConnect(remoteLibp2pID) ||
		fixture.controller.runtime.CanOpenDataPlane(remoteLibp2pID) {
		t.Fatal("hello ACK preceded durable pending binding and runtime authority installation")
	}

	fixture.controller.blockNextBaseline()
	baseline, err := NewDataBaseline(DataBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.remote.Identity().PeerID(),
		OriginEpoch:  fixture.remote.Identity().OriginEpoch(), BaselineChannelSequence: 7})
	if err != nil {
		t.Fatal(err)
	}
	stream := fixture.openStream(t, ctx)
	requestID := channelMemberRequestID(t, 0x12)
	frame, err := NewChannelFrame(requestID, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteChannelFrame(stream, frame); err != nil {
		t.Fatalf("write baseline request: %v", err)
	}
	type readResult struct {
		frame ChannelFrame
		err   error
	}
	responseC := make(chan readResult, 1)
	go func() {
		response, readErr := ReadChannelFrame(stream)
		responseC <- readResult{frame: response, err: readErr}
	}()
	select {
	case <-fixture.controller.baselineEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	readiness, err := fixture.store.ReadChannelBaselineReadiness(ctx,
		fixture.channel.Channel().ID())
	if err != nil || len(readiness) != 1 || readiness[0].InboundReady ||
		readiness[0].BindingState != model.BindingPending ||
		fixture.controller.runtime.CanOpenDataPlane(remoteLibp2pID) {
		t.Fatalf("pre-commit baseline authority = (%#v,%v)", readiness, err)
	}
	select {
	case early := <-responseC:
		t.Fatalf("baseline response arrived before commit/runtime install: (%#v,%v)", early.frame, early.err)
	case <-time.After(100 * time.Millisecond):
	}
	close(fixture.controller.baselineRelease)
	response := <-responseC
	if response.err != nil {
		t.Fatal(response.err)
	}
	ack, ok := response.frame.Payload().(DataBaselineAck)
	if response.frame.RequestID() != requestID || response.frame.Type() != ChannelFrameDataBaselineAck ||
		!ok || ack.BaselineChannelSequence() != 7 || !fixture.controller.baselineCommitted.Load() ||
		!fixture.controller.runtimeInstalled.Load() {
		t.Fatalf("post-commit baseline acknowledgement = %#v", response.frame)
	}
	readiness, err = fixture.store.ReadChannelBaselineReadiness(ctx, fixture.channel.Channel().ID())
	if err != nil || len(readiness) != 1 || !readiness[0].InboundReady ||
		readiness[0].BindingState != model.BindingActive ||
		!fixture.controller.runtime.CanOpenDataPlane(remoteLibp2pID) {
		t.Fatalf("post-ACK baseline authority = (%#v,%v)", readiness, err)
	}

	// Exact replay returns the same ACK without changing the durable baseline.
	replayed := fixture.roundTrip(t, ctx, 0x13, baseline)
	replayedAck, ok := replayed.Payload().(DataBaselineAck)
	if !ok || replayedAck.BaselineChannelSequence() != 7 {
		t.Fatalf("baseline replay response = %#v", replayed)
	}
	conflicting, err := NewDataBaseline(DataBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.remote.Identity().PeerID(),
		OriginEpoch:  fixture.remote.Identity().OriginEpoch(), BaselineChannelSequence: 8})
	if err != nil {
		t.Fatal(err)
	}
	conflict := fixture.roundTrip(t, ctx, 0x14, conflicting)
	assertChannelMemberFailure(t, conflict, ChannelErrorBaselineConflict, false, 0)
	otherEpoch, _ := model.ParseOriginEpoch("epoch-member-bootstrap-forged")
	wrongEpoch, err := NewDataBaseline(DataBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.remote.Identity().PeerID(), OriginEpoch: otherEpoch,
		BaselineChannelSequence: 7})
	if err != nil {
		t.Fatal(err)
	}
	epochFailure := fixture.roundTrip(t, ctx, 0x15, wrongEpoch)
	assertChannelMemberFailure(t, epochFailure, ChannelErrorOriginEpochMismatch, false, 0)
}

func TestChannelMemberServiceReturnsHelloSuffixAndFrozenSyncPages(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-pages")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture.bootstrapDirect(t, ctx)
	knownHead := fixture.remote.Member().Head()
	updates := make([]model.Member, 0, 18)
	for revision := 3; revision <= 20; revision++ {
		updates = append(updates,
			fixture.channel.AppendActiveUpdate(t, fixture.remote.Identity().PeerID()).Member())
	}
	merged, err := fixture.store.MergeChannelRoster(ctx, store.MergeChannelRosterSpec{
		ChannelID:                    fixture.channel.Channel().ID(),
		AuthenticatedTransportPeerID: fixture.remote.Identity().PeerID(),
		Records:                      updates, At: fixture.now,
	})
	if err != nil || merged.Status != store.ChannelRosterApplied || merged.Roster.Head().Revision() != 20 {
		t.Fatalf("prepare long roster = (%#v,%v)", merged, err)
	}
	if _, err := fixture.controller.refresh(ctx, fixture.channel.Channel().ID()); err != nil {
		t.Fatal(err)
	}
	service := fixture.service(t)
	dispatcher := fixture.serve(t, ctx, service)
	defer dispatcher.Close()

	hello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: knownHead,
		OwnerSignedProofChain: []model.Member{}})
	if err != nil {
		t.Fatal(err)
	}
	helloResponse := fixture.roundTrip(t, ctx, 0x21, hello)
	helloAck, ok := helloResponse.Payload().(MemberHelloAck)
	if !ok || helloAck.RosterHead().Revision() != 20 || len(helloAck.MissingRecords()) != 18 ||
		helloAck.MissingRecords()[0].Head().Revision() != 3 ||
		helloAck.MissingRecords()[17].Head().Revision() != 20 {
		t.Fatalf("hello missing suffix = %#v", helloResponse)
	}

	syncRequest, err := NewSyncRequest(SyncRequestSpec{ChannelID: fixture.channel.Channel().ID(),
		AfterHead: fixture.channel.OwnerMember().Member().Head()})
	if err != nil {
		t.Fatal(err)
	}
	stream := fixture.openStream(t, ctx)
	requestID := channelMemberRequestID(t, 0x22)
	requestFrame, _ := NewChannelFrame(requestID, syncRequest)
	if err := WriteChannelFrame(stream, requestFrame); err != nil {
		t.Fatal(err)
	}
	var pages []SyncPage
	for {
		response, err := ReadChannelFrame(stream)
		if err != nil {
			t.Fatal(err)
		}
		page, ok := response.Payload().(SyncPage)
		if response.RequestID() != requestID || !ok || page.RosterHead().Revision() != 20 {
			t.Fatalf("sync page = %#v", response)
		}
		pages = append(pages, page)
		if !page.More() {
			break
		}
	}
	if len(pages) != 2 || len(pages[0].OwnerSignedRecords()) != channelSyncPageRecordLimit ||
		!pages[0].More() || len(pages[1].OwnerSignedRecords()) != 3 || pages[1].More() ||
		pages[0].OwnerSignedRecords()[0].Head().Revision() != 2 ||
		pages[1].OwnerSignedRecords()[2].Head().Revision() != 20 {
		t.Fatalf("frozen sync pagination = %#v", pages)
	}

	ahead, _ := model.NewRecordHead(21, model.Sum([]byte("member-pages-ahead")))
	aheadHello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: ahead,
		OwnerSignedProofChain: []model.Member{}})
	if err != nil {
		t.Fatal(err)
	}
	aheadHelloResponse := fixture.roundTrip(t, ctx, 0x23, aheadHello)
	if _, ok := aheadHelloResponse.Payload().(MemberHelloAck); ok {
		t.Fatalf("hello ahead of local authority received success ACK: %#v", aheadHelloResponse)
	}
	assertChannelMemberFailure(t, aheadHelloResponse, ChannelErrorRosterGap, true,
		channelMemberGapRetry)

	gapRequest, _ := NewSyncRequest(SyncRequestSpec{ChannelID: fixture.channel.Channel().ID(),
		AfterHead: ahead})
	gap := fixture.roundTrip(t, ctx, 0x24, gapRequest)
	assertChannelMemberFailure(t, gap, ChannelErrorRosterGap, true, channelMemberGapRetry)
	wrongGenesis, _ := model.NewRecordHead(1, model.Sum([]byte("member-pages-wrong-genesis")))
	conflictRequest, _ := NewSyncRequest(SyncRequestSpec{ChannelID: fixture.channel.Channel().ID(),
		AfterHead: wrongGenesis})
	conflict := fixture.roundTrip(t, ctx, 0x25, conflictRequest)
	assertChannelMemberFailure(t, conflict, ChannelErrorRosterConflict, false, 0)

	revoked := fixture.channel.AppendTerminal(t, fixture.remote.Identity().PeerID(), model.MemberRevoked)
	revokedMerge, err := fixture.store.MergeChannelRoster(ctx, store.MergeChannelRosterSpec{
		ChannelID:                    fixture.channel.Channel().ID(),
		AuthenticatedTransportPeerID: fixture.remote.Identity().PeerID(),
		Records:                      []model.Member{revoked.Member()}, At: fixture.now,
	})
	if err != nil || revokedMerge.Status != store.ChannelRosterApplied {
		t.Fatalf("revoke remote = (%#v,%v)", revokedMerge, err)
	}
	revokedResponse := fixture.roundTrip(t, ctx, 0x26, syncRequest)
	assertChannelMemberFailure(t, revokedResponse, ChannelErrorMemberRevoked, false, 0)
}

func TestChannelMemberServiceUsesOnlySecureRemoteIdentity(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-identity")
	service := fixture.service(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dispatcher := fixture.serve(t, ctx, service)
	defer dispatcher.Close()

	attacker := testkit.NewIdentity(t, "member-identity-attacker")
	attackerHost := newEnrollmentTestHost(t, attacker)
	defer attackerHost.Close()
	if err := attackerHost.Connect(ctx, fixture.ownerAddrInfo()); err != nil {
		t.Fatal(err)
	}
	hello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: fixture.channel.Roster().Head(),
		OwnerSignedProofChain: fixture.channel.Roster().Members()})
	if err != nil {
		t.Fatal(err)
	}
	stream := openEnrollmentTestStream(t, ctx, attackerHost, fixture.ownerHost.ID())
	_ = stream.SetDeadline(time.Now().Add(2 * time.Second))
	frame, _ := NewChannelFrame(channelMemberRequestID(t, 0x31), hello)
	if err := WriteChannelFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	if response, err := ReadChannelFrame(stream); err == nil || !response.IsZero() {
		t.Fatalf("claimed message identity received a response: (%#v,%v)", response, err)
	}
	if fixture.controller.helloCalls.Load() != 0 {
		t.Fatal("message-claimed PeerID reached controller before secure identity check")
	}

	controllerType := reflect.TypeOf((*ChannelMemberController)(nil)).Elem()
	if reflect.TypeOf((*store.Store)(nil)).Implements(controllerType) {
		t.Fatal("*store.Store accidentally satisfies composite ChannelMemberController")
	}
	var nilController *storeBackedChannelMemberController
	if service, err := NewChannelMemberService(ChannelMemberServiceOptions{Controller: nilController}); service != nil || !errors.Is(err, ErrChannelMemberProtocol) {
		t.Fatalf("typed nil controller = (%#v,%v)", service, err)
	}
}

type channelMemberStoreFixture struct {
	store      *store.Store
	channel    *testkit.SignedChannel
	remote     testkit.MemberFixture
	controller *storeBackedChannelMemberController
	ownerHost  host.Host
	remoteHost host.Host
	now        time.Time
}

func newChannelMemberStoreFixture(t *testing.T, seed string) *channelMemberStoreFixture {
	t.Helper()
	createdAt := time.Date(2026, 7, 19, 1, 0, 0, 0, time.UTC)
	now := createdAt.Add(24 * time.Hour)
	owner := testkit.NewIdentity(t, seed+"-owner")
	channel := testkit.NewSignedChannelForOwnerAt(t, seed, owner, createdAt)
	st := openPeerMeshStore(t, owner, createdAt)
	createPeerMeshChannel(t, st, channel, seed)
	remote := channel.AppendActive(t, seed+"-remote")
	mesh, err := st.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := ProjectNetworkAuthority(mesh)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewAuthority(owner.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Replace(snapshot); err != nil {
		t.Fatalf("initialize fake runtime authority: %v", err)
	}
	controller := &storeBackedChannelMemberController{store: st, runtime: runtime}
	ownerHost := newEnrollmentTestHost(t, owner)
	remoteHost := newEnrollmentTestHost(t, remote.Identity())
	t.Cleanup(func() {
		_ = remoteHost.Close()
		_ = ownerHost.Close()
	})
	return &channelMemberStoreFixture{store: st, channel: channel, remote: remote,
		controller: controller, ownerHost: ownerHost, remoteHost: remoteHost, now: now}
}

func (fixture *channelMemberStoreFixture) service(t *testing.T) *ChannelMemberService {
	t.Helper()
	service, err := NewChannelMemberService(ChannelMemberServiceOptions{
		Controller: fixture.controller, Clock: fixedChannelMemberClock{at: fixture.now}})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func (fixture *channelMemberStoreFixture) serve(t *testing.T, ctx context.Context,
	service *ChannelMemberService,
) *ChannelDispatcher {
	t.Helper()
	if err := fixture.remoteHost.Connect(ctx, fixture.ownerAddrInfo()); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewChannelDispatcher(ctx, fixture.ownerHost, ChannelDispatcherOptions{
		Enrollment: ChannelRequestHandlerFunc(func(context.Context, network.Stream, ChannelFrame) error {
			return errors.New("unexpected enrollment request")
		}),
		Member: service,
	})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher
}

func (fixture *channelMemberStoreFixture) ownerAddrInfo() libp2ppeer.AddrInfo {
	return libp2ppeer.AddrInfo{ID: fixture.ownerHost.ID(), Addrs: fixture.ownerHost.Addrs()}
}

func (fixture *channelMemberStoreFixture) openStream(t *testing.T,
	ctx context.Context,
) network.Stream {
	t.Helper()
	return openEnrollmentTestStream(t, ctx, fixture.remoteHost, fixture.ownerHost.ID())
}

func (fixture *channelMemberStoreFixture) roundTrip(t *testing.T, ctx context.Context,
	requestByte byte, payload ChannelFramePayload,
) ChannelFrame {
	t.Helper()
	stream := fixture.openStream(t, ctx)
	defer stream.Close()
	requestID := channelMemberRequestID(t, requestByte)
	request, err := NewChannelFrame(requestID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteChannelFrame(stream, request); err != nil {
		t.Fatal(err)
	}
	response, err := ReadChannelFrame(stream)
	if err != nil || response.RequestID() != requestID {
		t.Fatalf("Channel member round trip = (%#v,%v)", response, err)
	}
	return response
}

func (fixture *channelMemberStoreFixture) bootstrapDirect(t *testing.T, ctx context.Context) {
	t.Helper()
	result, err := fixture.controller.ReconcileMemberHelloGate(ctx, ChannelMemberHelloControl{
		AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
		ChannelID:           fixture.channel.Channel().ID(), ActiveMemberRecord: fixture.remote.Member(),
		KnownRosterHead: fixture.channel.Roster().Head(),
		ProofRecords:    fixture.channel.Roster().Members(), At: fixture.now,
	})
	if err != nil || result.Roster.Head() != fixture.channel.Roster().Head() {
		t.Fatalf("direct member bootstrap = (%#v,%v)", result, err)
	}
}

type fixedChannelMemberClock struct{ at time.Time }

func (clock fixedChannelMemberClock) Now() time.Time { return clock.at }

type storeBackedChannelMemberController struct {
	store   *store.Store
	runtime *Authority
	mu      sync.Mutex

	helloCalls        atomic.Int32
	helloCommitted    atomic.Bool
	baselineCommitted atomic.Bool
	runtimeInstalled  atomic.Bool
	blockBaseline     atomic.Bool
	baselineEntered   chan struct{}
	baselineRelease   chan struct{}
}

func (controller *storeBackedChannelMemberController) ReconcileMemberHelloGate(ctx context.Context,
	control ChannelMemberHelloControl,
) (ChannelMemberHelloAuthority, error) {
	controller.helloCalls.Add(1)
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if len(control.ProofRecords) > 0 {
		result, err := controller.store.MergeChannelRoster(ctx, store.MergeChannelRosterSpec{
			ChannelID:                    control.ChannelID,
			AuthenticatedTransportPeerID: control.AuthenticatedPeerID,
			Records:                      control.ProofRecords, At: control.At,
		})
		if err != nil {
			return ChannelMemberHelloAuthority{}, mapStoreBackedMemberError(err)
		}
		switch result.Status {
		case store.ChannelRosterApplied, store.ChannelRosterDuplicate:
			controller.helloCommitted.Store(true)
		case store.ChannelRosterGap:
			return ChannelMemberHelloAuthority{}, ErrChannelMemberRosterGap
		case store.ChannelRosterConflicted:
			return ChannelMemberHelloAuthority{}, ErrChannelMemberRosterConflict
		default:
			return ChannelMemberHelloAuthority{}, errors.New("unknown roster merge status")
		}
	}
	roster, err := controller.refreshLocked(ctx, control.ChannelID)
	if err != nil {
		return ChannelMemberHelloAuthority{}, err
	}
	return ChannelMemberHelloAuthority{Roster: roster}, nil
}

func (controller *storeBackedChannelMemberController) FreezeMemberRosterForSync(ctx context.Context,
	control ChannelMemberSyncControl,
) (ChannelMemberRosterSnapshot, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	roster, err := controller.readRosterLocked(ctx, control.ChannelID)
	if err != nil {
		return ChannelMemberRosterSnapshot{}, err
	}
	return ChannelMemberRosterSnapshot{Roster: roster}, nil
}

func (controller *storeBackedChannelMemberController) InstallMemberBaselineGate(ctx context.Context,
	control ChannelMemberBaselineControl,
) (ChannelMemberBaselineAuthority, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.blockBaseline.CompareAndSwap(true, false) {
		close(controller.baselineEntered)
		select {
		case <-controller.baselineRelease:
		case <-ctx.Done():
			return ChannelMemberBaselineAuthority{}, ctx.Err()
		}
	}
	result, err := controller.store.InstallInboundChannelBaseline(ctx,
		store.InstallInboundChannelBaselineSpec{AuthenticatedPeerID: control.AuthenticatedPeerID,
			Baseline: store.ChannelDataBaseline{ChannelID: control.Baseline.ChannelID,
				OriginPeerID:            control.Baseline.OriginPeerID,
				OriginEpoch:             control.Baseline.OriginEpoch,
				BaselineChannelSequence: control.Baseline.BaselineChannelSequence},
			At: control.At})
	if err != nil {
		return ChannelMemberBaselineAuthority{}, mapStoreBackedMemberError(err)
	}
	controller.baselineCommitted.Store(true)
	roster, err := controller.refreshLocked(ctx, control.Baseline.ChannelID)
	if err != nil {
		return ChannelMemberBaselineAuthority{}, err
	}
	committed := DataBaselineSpec{ChannelID: result.Baseline.ChannelID,
		OriginPeerID: result.Baseline.OriginPeerID, OriginEpoch: result.Baseline.OriginEpoch,
		BaselineChannelSequence: result.Baseline.BaselineChannelSequence}
	return ChannelMemberBaselineAuthority{Baseline: committed, Roster: roster}, nil
}

func (controller *storeBackedChannelMemberController) blockNextBaseline() {
	controller.baselineEntered = make(chan struct{})
	controller.baselineRelease = make(chan struct{})
	controller.blockBaseline.Store(true)
}

func (controller *storeBackedChannelMemberController) refresh(ctx context.Context,
	channelID model.ChannelID,
) (model.VerifiedRoster, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.refreshLocked(ctx, channelID)
}

func (controller *storeBackedChannelMemberController) refreshLocked(ctx context.Context,
	channelID model.ChannelID,
) (model.VerifiedRoster, error) {
	mesh, err := controller.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return model.VerifiedRoster{}, err
	}
	snapshot, err := ProjectNetworkAuthority(mesh)
	if err != nil {
		return model.VerifiedRoster{}, err
	}
	if err := controller.runtime.Replace(snapshot); err != nil {
		return model.VerifiedRoster{}, err
	}
	controller.runtimeInstalled.Store(true)
	for _, channel := range mesh.Channels() {
		if channel.Channel().ID() == channelID {
			return channel.Roster(), nil
		}
	}
	return model.VerifiedRoster{}, ErrChannelMemberNotMember
}

func (controller *storeBackedChannelMemberController) readRosterLocked(ctx context.Context,
	channelID model.ChannelID,
) (model.VerifiedRoster, error) {
	mesh, err := controller.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return model.VerifiedRoster{}, err
	}
	for _, channel := range mesh.Channels() {
		if channel.Channel().ID() == channelID {
			return channel.Roster(), nil
		}
	}
	return model.VerifiedRoster{}, ErrChannelMemberNotMember
}

func mapStoreBackedMemberError(err error) error {
	switch {
	case errors.Is(err, store.ErrChannelBaselineConflict):
		return ErrChannelMemberBaselineConflict
	case errors.Is(err, store.ErrChannelBaselineEpochMismatch):
		return ErrChannelMemberEpochMismatch
	case errors.Is(err, store.ErrChannelRosterConflict):
		return ErrChannelMemberRosterConflict
	case errors.Is(err, store.ErrChannelRosterInput):
		return ErrChannelMemberRosterConflict
	case errors.Is(err, store.ErrChannelBaselineAuthority):
		return ErrChannelMemberNotMember
	default:
		return err
	}
}

func channelMemberRequestID(t *testing.T, value byte) ChannelRequestID {
	t.Helper()
	requestID, err := NewChannelRequestID(bytes.NewReader(bytes.Repeat([]byte{value},
		channelRequestIDBytes)))
	if err != nil {
		t.Fatal(err)
	}
	return requestID
}

func assertChannelMemberFailure(t *testing.T, frame ChannelFrame,
	code ChannelProtocolErrorCode, retryable bool, retryAfter time.Duration,
) {
	t.Helper()
	payload, ok := frame.Payload().(ProtocolError)
	if frame.Type() != ChannelFrameProtocolError || !ok || payload.Code() != code ||
		payload.Retryable() != retryable || payload.RetryAfter() != retryAfter {
		t.Fatalf("Channel member failure = %#v, want %s", frame, code)
	}
}
