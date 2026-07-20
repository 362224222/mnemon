package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestMeshRuntimeJoinChannelBindsPreparedPermitInitAndMarksBeforeProof(t *testing.T) {
	fixture := newChannelJoinBindingFixture(t, "peer-join-binding")
	markEntered := make(chan struct{})
	resumeMark := make(chan struct{})
	session := &recordingChannelJoinSession{epoch: fixture.joiner.OriginEpoch(), reserved: true,
		markEntered: markEntered, resumeMark: resumeMark}
	initSeen := make(chan model.EnrollmentRequestID, 1)
	proofSeen := make(chan struct{})
	registerChannelJoinTestHandler(t, fixture.ctx, fixture.ownerHost,
		func(_ context.Context, stream network.Stream, frame ChannelFrame) error {
			init, ok := frame.Payload().(EnrollInit)
			if !ok {
				return errors.New("join binding handler received non-init frame")
			}
			initSeen <- init.EnrollmentRequestID()
			if err := writeChannelJoinTestChallenge(stream, frame.RequestID(),
				fixture.channel.Channel().RosterHead()); err != nil {
				return err
			}
			proof, release, err := readChannelStreamFrame(stream, model.MaxChannelRecordBytes)
			if err != nil {
				return err
			}
			release()
			if proof.RequestID() != frame.RequestID() || proof.Type() != ChannelFrameEnrollProof {
				return errors.New("join binding handler received invalid proof")
			}
			close(proofSeen)
			return writeChannelJoinTestFailure(stream, frame.RequestID(), ChannelErrorTokenClosed)
		})
	type joinOutcome struct {
		result ChannelJoinResult
		err    error
	}
	joined := make(chan joinOutcome, 1)
	go func() {
		result, err := fixture.runtime.JoinChannel(fixture.ctx, fixture.spec, session)
		joined <- joinOutcome{result: result, err: err}
	}()
	assertChannelJoinBlocksProofUntilMark(t, fixture, session, markEntered, initSeen, proofSeen)
	close(resumeMark)
	outcome := <-joined
	assertChannelJoinStableRejection(t, fixture, session, outcome.result, outcome.err, proofSeen)
}

func assertChannelJoinBlocksProofUntilMark(t *testing.T, fixture channelJoinBindingFixture,
	session *recordingChannelJoinSession, markEntered <-chan struct{},
	initSeen <-chan model.EnrollmentRequestID, proofSeen <-chan struct{},
) {
	t.Helper()
	select {
	case <-markEntered:
	case <-fixture.ctx.Done():
		t.Fatal(fixture.ctx.Err())
	}
	select {
	case <-proofSeen:
		t.Fatal("proof was transmitted before commit_unknown completed")
	default:
	}
	prepared := session.preparedJoin()
	select {
	case initRequest := <-initSeen:
		if initRequest != prepared.RequestID() {
			t.Fatalf("EnrollInit request = %s, prepared %s", initRequest, prepared.RequestID())
		}
	default:
		t.Fatal("handler had not received EnrollInit before Mark callback")
	}
	fixture.runtime.nodeHost.gater.mu.Lock()
	if len(fixture.runtime.nodeHost.gater.outbound.permits) != 1 {
		fixture.runtime.nodeHost.gater.mu.Unlock()
		t.Fatal("expected one live enrollment permit while Mark was blocked")
	}
	for key := range fixture.runtime.nodeHost.gater.outbound.permits {
		if key.enrollmentRequestID != prepared.RequestID().String() {
			fixture.runtime.nodeHost.gater.mu.Unlock()
			t.Fatalf("permit request = %s, prepared %s", key.enrollmentRequestID,
				prepared.RequestID())
		}
	}
	fixture.runtime.nodeHost.gater.mu.Unlock()
}

func assertChannelJoinStableRejection(t *testing.T, fixture channelJoinBindingFixture,
	session *recordingChannelJoinSession, result ChannelJoinResult, err error,
	proofSeen <-chan struct{},
) {
	t.Helper()
	var failure *ChannelProtocolFailure
	if result.Status().Valid() || !errors.As(err, &failure) ||
		failure.Code() != ChannelErrorTokenClosed {
		t.Fatalf("joined outcome = (%#v,%v)", result, err)
	}
	select {
	case <-proofSeen:
	default:
		t.Fatal("proof was not transmitted after Mark callback completed")
	}
	if begun, marked, released := session.counts(); begun != 1 || marked != 1 || released != 1 {
		t.Fatalf("session calls = begin %d, mark %d, release %d", begun, marked, released)
	}
	if slots := fixture.runtime.nodeHost.gater.UnknownEnrollmentSlots(); slots != 0 {
		t.Fatalf("stable rejection retained %d enrollment permit slots", slots)
	}
}

func TestMeshRuntimeJoinChannelObservesFreshPreproofReleaseFailure(t *testing.T) {
	fixture := newChannelJoinBindingFixture(t, "peer-join-fresh-release-failure")
	session := &recordingChannelJoinSession{epoch: fixture.joiner.OriginEpoch(), reserved: true,
		mismatchRequest: true, releaseErr: context.DeadlineExceeded}
	result, err := fixture.runtime.JoinChannel(fixture.ctx, fixture.spec, session)
	var failure *ChannelProtocolFailure
	if result.Status().Valid() || !errors.As(err, &failure) ||
		failure.Code() != ChannelErrorRosterConflict || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fresh release failure = (%#v,%v)", result, err)
	}
	if begun, marked, released := session.counts(); begun != 1 || marked != 0 || released != 1 {
		t.Fatalf("fresh release calls = begin %d, mark %d, release %d", begun, marked, released)
	}
	if slots := fixture.runtime.nodeHost.gater.UnknownEnrollmentSlots(); slots != 0 {
		t.Fatalf("pre-dial binding failure acquired %d transport slots", slots)
	}
}

func TestMeshRuntimeJoinChannelObservesAuthenticatedRejectionReleaseFailure(t *testing.T) {
	fixture := newChannelJoinBindingFixture(t, "peer-join-stable-release-failure")
	session := &recordingChannelJoinSession{epoch: fixture.joiner.OriginEpoch(), reserved: true,
		commitUnknown: true, releaseErr: context.DeadlineExceeded}
	registerChannelJoinTestHandler(t, fixture.ctx, fixture.ownerHost,
		func(_ context.Context, stream network.Stream, frame ChannelFrame) error {
			return writeChannelJoinTestFailure(stream, frame.RequestID(), ChannelErrorTokenClosed)
		})
	result, err := fixture.runtime.JoinChannel(fixture.ctx, fixture.spec, session)
	var failure *ChannelProtocolFailure
	if result.Status().Valid() || !errors.As(err, &failure) ||
		failure.Code() != ChannelErrorTokenClosed || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("authenticated release failure = (%#v,%v)", result, err)
	}
	if begun, marked, released := session.counts(); begun != 1 || marked != 0 || released != 1 {
		t.Fatalf("authenticated release calls = begin %d, mark %d, release %d",
			begun, marked, released)
	}
}

func TestMeshRuntimeJoinChannelReturnsBusyWhenEnrollmentBudgetIsFull(t *testing.T) {
	fixture := newChannelJoinBindingFixture(t, "peer-join-budget-busy")
	permits := make([]*enrollmentTransportPermit, 0,
		HermeticLimits().UnknownEnrollmentConnections)
	for index := 0; index < HermeticLimits().UnknownEnrollmentConnections; index++ {
		requestID, err := model.ParseEnrollmentRequestID(
			fmt.Sprintf("request-peer-join-budget-busy-%d", index))
		if err != nil {
			t.Fatal(err)
		}
		permit, err := fixture.runtime.acquireEnrollmentTransportPermit(fixture.ctx,
			enrollmentTransportPermitRequest{Token: fixture.spec.Token,
				EnrollmentRequestID: requestID})
		if err != nil {
			t.Fatalf("fill enrollment budget %d: %v", index, err)
		}
		permits = append(permits, permit)
	}
	assertChannelJoinBusyBeforeDial(t, fixture)
	for _, permit := range permits {
		if err := permit.Close(); err != nil {
			t.Fatalf("release budget permit: %v", err)
		}
	}
}

func TestMeshRuntimeJoinChannelReturnsBusyForDuplicatePreparedAttempt(t *testing.T) {
	fixture := newChannelJoinBindingFixture(t, "peer-join-attempt-busy")
	identity, err := model.EnrollmentJoinIdentityDigest(fixture.channel.Channel().ID(),
		fixture.spec.Token.Payload().GrantID(), fixture.joiner.PeerID(), fixture.joiner.PublicKey(),
		fixture.joiner.OriginEpoch())
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := model.EnrollmentRequestIDForJoinIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := fixture.runtime.acquireEnrollmentTransportPermit(fixture.ctx,
		enrollmentTransportPermitRequest{Token: fixture.spec.Token,
			EnrollmentRequestID: requestID})
	if err != nil {
		t.Fatal(err)
	}
	assertChannelJoinBusyBeforeDial(t, fixture)
	if err := permit.Close(); err != nil {
		t.Fatalf("release duplicate permit: %v", err)
	}
}

func assertChannelJoinBusyBeforeDial(t *testing.T, fixture channelJoinBindingFixture) {
	t.Helper()
	session := &recordingChannelJoinSession{epoch: fixture.joiner.OriginEpoch(), reserved: true}
	result, err := fixture.runtime.JoinChannel(fixture.ctx, fixture.spec, session)
	var failure *ChannelProtocolFailure
	if result.Status().Valid() || !errors.As(err, &failure) ||
		failure.Code() != ChannelErrorBusy || !failure.Retryable() ||
		failure.RetryAfter() != channelEnrollmentBusyRetry {
		t.Fatalf("bounded admission = (%#v,%v)", result, err)
	}
	if begun, marked, released := session.counts(); begun != 1 || marked != 0 || released != 1 {
		t.Fatalf("bounded admission calls = begin %d, mark %d, release %d",
			begun, marked, released)
	}
}

type channelJoinBindingFixture struct {
	ctx       context.Context
	runtime   *MeshRuntime
	ownerHost host.Host
	channel   *testkit.SignedChannel
	joiner    testkit.Identity
	spec      JoinChannelSpec
}

func newChannelJoinBindingFixture(t *testing.T, seed string) channelJoinBindingFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	createdAt := time.Date(2026, 7, 20, 4, 0, 0, 0, time.UTC)
	channel := testkit.NewSignedChannelAt(t, seed+"-channel", createdAt)
	ownerHost := newEnrollmentTestHostAtIdentityAddress(t, channel.Owner())
	t.Cleanup(func() {
		if err := ownerHost.Close(); err != nil {
			t.Errorf("close join test owner: %v", err)
		}
	})
	joiner := testkit.NewIdentity(t, seed+"-joiner")
	joinerStore := openEnrollmentTestStore(t, joiner, createdAt)
	runtime := newTestMeshRuntime(t, ctx, joiner, readMeshRuntimeAuthority(t, joinerStore))
	grantID, err := model.ParseGrantID("grant-" + seed)
	if err != nil {
		t.Fatal(err)
	}
	token := enrollmentTestTokenForHost(t, channel, grantID, seed, ownerHost)
	return channelJoinBindingFixture{ctx: ctx, runtime: runtime, ownerHost: ownerHost,
		channel: channel, joiner: joiner,
		spec: JoinChannelSpec{Token: token, DisplayLabel: joiner.DisplayName(),
			AdvertisedMultiaddrs: joiner.Multiaddrs(), LocalAlias: seed + "-alias"}}
}

func registerChannelJoinTestHandler(t *testing.T, ctx context.Context, ownerHost host.Host,
	handler ChannelRequestHandlerFunc,
) {
	t.Helper()
	dispatcher, err := NewChannelDispatcher(ctx, ownerHost,
		ChannelDispatcherOptions{Enrollment: handler})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := dispatcher.Close(); err != nil {
			t.Errorf("close join test dispatcher: %v", err)
		}
	})
}

func writeChannelJoinTestChallenge(stream network.Stream, requestID ChannelRequestID,
	rosterHead model.RecordHead,
) error {
	challenge, err := NewEnrollChallenge(EnrollChallengeSpec{
		OwnerNonce: bytesOf(0x51, model.EnrollmentNonceBytes), SelectedVersion: ChannelFrameVersion,
		Limits: model.DefaultMemberLimits(), RosterHead: rosterHead})
	if err != nil {
		return err
	}
	frame, err := NewChannelFrame(requestID, challenge)
	if err != nil {
		return err
	}
	return WriteChannelFrame(stream, frame)
}

func writeChannelJoinTestFailure(stream network.Stream, requestID ChannelRequestID,
	code ChannelProtocolErrorCode,
) error {
	failure, err := NewProtocolError(ProtocolErrorSpec{Code: code})
	if err != nil {
		return err
	}
	frame, err := NewChannelFrame(requestID, failure)
	if err != nil {
		return err
	}
	return WriteChannelFrame(stream, frame)
}

func bytesOf(value byte, count int) []byte {
	result := make([]byte, count)
	for index := range result {
		result[index] = value
	}
	return result
}

type recordingChannelJoinSession struct {
	epoch           model.OriginEpoch
	reserved        bool
	commitUnknown   bool
	mismatchRequest bool
	releaseErr      error
	markEntered     chan struct{}
	resumeMark      chan struct{}

	mu       sync.Mutex
	begin    int
	mark     int
	release  int
	prepared PreparedChannelJoin
}

func (session *recordingChannelJoinSession) BeginChannelJoin(_ context.Context,
	control ChannelJoinPrepareControl,
) (PreparedChannelJoin, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.begin != 0 {
		return PreparedChannelJoin{}, errors.New("recording join session reused")
	}
	session.begin++
	identity, err := model.EnrollmentJoinIdentityDigest(control.Descriptor.Descriptor().ID(),
		control.GrantID, control.AuthenticatedLocalPeerID, control.LocalPublicKey, session.epoch)
	if err != nil {
		return PreparedChannelJoin{}, err
	}
	requestID, err := model.EnrollmentRequestIDForJoinIdentity(identity)
	if err != nil {
		return PreparedChannelJoin{}, err
	}
	if session.mismatchRequest {
		requestID, err = model.ParseEnrollmentRequestID("request-intentionally-mismatched")
		if err != nil {
			return PreparedChannelJoin{}, err
		}
	}
	prepared, err := NewPreparedChannelJoin(requestID, session.epoch,
		session.reserved, session.commitUnknown)
	if err == nil {
		session.prepared = prepared
	}
	return prepared, err
}

func (session *recordingChannelJoinSession) MarkChannelJoinCommitUnknown(ctx context.Context,
	_ time.Time,
) error {
	session.mu.Lock()
	session.mark++
	entered := session.markEntered
	resume := session.resumeMark
	session.mu.Unlock()
	if entered != nil {
		close(entered)
	}
	if resume != nil {
		select {
		case <-resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (session *recordingChannelJoinSession) ReleaseChannelJoinReservation(context.Context) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.release++
	return session.releaseErr
}

func (*recordingChannelJoinSession) InstallAcceptedChannelJoin(context.Context,
	VerifiedChannelEnrollment, time.Time,
) (ChannelJoinResult, error) {
	return ChannelJoinResult{}, errors.New("recording join session unexpectedly installed authority")
}

func (session *recordingChannelJoinSession) preparedJoin() PreparedChannelJoin {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.prepared
}

func (session *recordingChannelJoinSession) counts() (int, int, int) {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.begin, session.mark, session.release
}

var _ ChannelJoinSession = (*recordingChannelJoinSession)(nil)
