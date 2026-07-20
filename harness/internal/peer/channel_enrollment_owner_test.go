package peer

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelEnrollmentOwnerRejectsUnsupportedVersionBeforeController(t *testing.T) {
	fixture := testkit.NewSignedChannel(t, "peer-enrollment-version")
	joiner := testkit.NewIdentity(t, "peer-enrollment-version-joiner")
	ownerHost := newEnrollmentTestHost(t, fixture.Owner())
	defer ownerHost.Close()
	joinerHost := newEnrollmentTestHost(t, joiner)
	defer joinerHost.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := joinerHost.Connect(ctx, libp2ppeer.AddrInfo{ID: ownerHost.ID(),
		Addrs: ownerHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	called := make(chan struct{}, 1)
	ownerProtocol, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{
		Controller: unexpectedEnrollmentOwnerController{called: called},
	})
	if err != nil {
		t.Fatal(err)
	}
	registerEnrollmentTestDispatcher(t, ctx, ownerHost, ownerProtocol)
	grantID, _ := model.ParseGrantID("grant-peer-enrollment-version")
	identity, err := model.EnrollmentJoinIdentityDigest(fixture.Channel().ID(), grantID,
		joiner.PeerID(), joiner.PublicKey(), joiner.OriginEpoch())
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := model.EnrollmentRequestIDForJoinIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	init, err := NewEnrollInit(EnrollInitSpec{ChannelID: fixture.Channel().ID(), GrantID: grantID,
		EnrollmentRequestID: requestID,
		JoinerNonce:         bytes.Repeat([]byte{0x55}, model.EnrollmentNonceBytes),
		SupportedVersions:   []uint8{2}, OriginEpoch: joiner.OriginEpoch(),
		DisplayLabel: joiner.DisplayName(), AdvertisedMultiaddrs: joiner.Multiaddrs()})
	if err != nil {
		t.Fatal(err)
	}
	frameRequestID, err := ParseChannelRequestID("channel-request-303132333435363738393a3b3c3d3e3f")
	if err != nil {
		t.Fatal(err)
	}
	frame, err := NewChannelFrame(frameRequestID, init)
	if err != nil {
		t.Fatal(err)
	}
	stream := openEnrollmentTestStream(t, ctx, joinerHost, ownerHost.ID())
	defer stream.Close()
	if err := WriteChannelFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	response, err := ReadChannelFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	failure, ok := response.Payload().(ProtocolError)
	if response.RequestID() != frameRequestID || !ok ||
		failure.Code() != ChannelErrorIncompatibleProtocol || failure.Retryable() {
		t.Fatalf("unsupported-version response = %#v", response)
	}
	select {
	case <-called:
		t.Fatal("unsupported version reached owner controller")
	default:
	}
}

func TestChannelEnrollmentOwnerRejectsTypedNilController(t *testing.T) {
	var controller *unexpectedEnrollmentOwnerController
	owner, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{Controller: controller})
	if owner != nil || err == nil {
		t.Fatalf("typed-nil controller owner = (%#v,%v)", owner, err)
	}
}

type unexpectedEnrollmentOwnerController struct{ called chan struct{} }

func (unexpected unexpectedEnrollmentOwnerController) PrepareEnrollmentChallenge(context.Context,
	ChannelEnrollmentChallengeControl,
) (ChannelEnrollmentChallengeAuthority, error) {
	select {
	case unexpected.called <- struct{}{}:
	default:
	}
	return ChannelEnrollmentChallengeAuthority{}, enrollmentOwnerControlFailure(ChannelErrorInvalidToken)
}

func (unexpected unexpectedEnrollmentOwnerController) AcceptEnrollmentAuthority(context.Context,
	ChannelEnrollmentAcceptanceControl,
) (ChannelEnrollmentAcceptanceAuthority, error) {
	select {
	case unexpected.called <- struct{}{}:
	default:
	}
	return ChannelEnrollmentAcceptanceAuthority{}, enrollmentOwnerControlFailure(ChannelErrorInvalidToken)
}

type enrollmentOwnerTestStore interface {
	PrepareChannelEnrollment(context.Context,
		store.PrepareChannelEnrollmentSpec,
	) (store.PrepareChannelEnrollmentResult, error)
	AcceptChannelEnrollment(context.Context,
		store.AcceptChannelEnrollmentSpec,
	) (store.AcceptChannelEnrollmentResult, error)
}

// enrollmentOwnerTestController is the test composition adapter. Production
// peer code never sees this Store or signer capability; mnemond will own the
// equivalent mapping and authority gate.
type enrollmentOwnerTestController struct {
	store  enrollmentOwnerTestStore
	signer store.ChannelAuthoritySigner
}

func enrollmentOwnerTestControllerFor(t *testing.T, st enrollmentOwnerTestStore,
	identity testkit.Identity,
) enrollmentOwnerTestController {
	t.Helper()
	return enrollmentOwnerTestController{store: st,
		signer: enrollmentTestSigner{privateKey: enrollmentPrivateKey(t, identity)}}
}

func (controller enrollmentOwnerTestController) PrepareEnrollmentChallenge(ctx context.Context,
	control ChannelEnrollmentChallengeControl,
) (ChannelEnrollmentChallengeAuthority, error) {
	result, err := controller.store.PrepareChannelEnrollment(ctx,
		store.PrepareChannelEnrollmentSpec{ChannelID: control.ChannelID,
			GrantID: control.GrantID, RequestID: control.RequestID,
			AuthenticatedPeerID: control.AuthenticatedPeerID,
			JoinerOriginEpoch:   control.JoinerOriginEpoch,
			JoinerPublicKey:     append([]byte(nil), control.JoinerPublicKey...), At: control.At})
	if err != nil {
		return ChannelEnrollmentChallengeAuthority{}, enrollmentOwnerTestControlFailure(err)
	}
	return ChannelEnrollmentChallengeAuthority{RosterHead: result.RosterHead}, nil
}

func (controller enrollmentOwnerTestController) AcceptEnrollmentAuthority(ctx context.Context,
	control ChannelEnrollmentAcceptanceControl,
) (ChannelEnrollmentAcceptanceAuthority, error) {
	result, err := controller.store.AcceptChannelEnrollment(ctx, store.AcceptChannelEnrollmentSpec{
		AuthenticatedPeerID: control.AuthenticatedPeerID, Transcript: control.Transcript,
		AdvertisedMultiaddrs: append([]string(nil), control.AdvertisedMultiaddrs...),
		Proof:                control.Proof, Signer: controller.signer, At: control.At,
	})
	if err != nil {
		return ChannelEnrollmentAcceptanceAuthority{}, enrollmentOwnerTestControlFailure(err)
	}
	status, err := enrollmentOwnerTestStatus(result.Status)
	if err != nil {
		return ChannelEnrollmentAcceptanceAuthority{}, err
	}
	return ChannelEnrollmentAcceptanceAuthority{Status: status, Member: result.Member,
		Roster: result.Roster.Members(), Receipt: result.Receipt}, nil
}

func enrollmentOwnerTestStatus(status store.ChannelEnrollmentStatus) (ChannelEnrollmentStatus, error) {
	switch status {
	case store.ChannelEnrollmentAccepted:
		return ChannelEnrollmentAccepted, nil
	case store.ChannelEnrollmentReplayed:
		return ChannelEnrollmentReplayed, nil
	case store.ChannelEnrollmentMemberRevoked:
		return ChannelEnrollmentMemberRevoked, nil
	case store.ChannelEnrollmentChannelClosed:
		return ChannelEnrollmentChannelClosed, nil
	default:
		return "", ErrChannelEnrollmentProtocol
	}
}

func enrollmentOwnerTestControlFailure(cause error) error {
	var code ChannelProtocolErrorCode
	switch {
	case errors.Is(cause, store.ErrChannelEnrollmentOwner):
		code = ChannelErrorWrongOwner
	case errors.Is(cause, store.ErrChannelEnrollmentProof):
		code = ChannelErrorBadProof
	case errors.Is(cause, store.ErrChannelEnrollmentTokenExpired):
		code = ChannelErrorTokenExpired
	case errors.Is(cause, store.ErrChannelEnrollmentTokenClosed):
		code = ChannelErrorTokenClosed
	case errors.Is(cause, store.ErrChannelEnrollmentTokenExhausted):
		code = ChannelErrorTokenExhausted
	case errors.Is(cause, store.ErrChannelFull):
		code = ChannelErrorChannelFull
	case errors.Is(cause, store.ErrChannelEnrollmentChannelClosed):
		code = ChannelErrorChannelClosed
	case errors.Is(cause, store.ErrChannelEnrollmentMemberRevoked):
		code = ChannelErrorMemberRevoked
	case errors.Is(cause, store.ErrChannelEnrollmentStale):
		code = ChannelErrorRosterGap
	case errors.Is(cause, store.ErrChannelEnrollmentConflict),
		errors.Is(cause, store.ErrChannelAuthorityInvariant):
		code = ChannelErrorRosterConflict
	case errors.Is(cause, store.ErrChannelEnrollmentUnavailable),
		errors.Is(cause, store.ErrChannelEnrollmentInput):
		code = ChannelErrorInvalidToken
	default:
		return cause
	}
	return enrollmentOwnerControlFailure(code)
}

func enrollmentOwnerControlFailure(code ChannelProtocolErrorCode) error {
	failure, err := NewChannelEnrollmentControlFailure(code)
	if err != nil {
		return err
	}
	return failure
}
