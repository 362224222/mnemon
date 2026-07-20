package peer

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestReceivedChannelFailureIsStableOnlyForExactAuthenticatedRequest(t *testing.T) {
	requestID, err := NewChannelRequestID(bytes.NewReader(bytesOf(0x61, channelRequestIDBytes)))
	if err != nil {
		t.Fatal(err)
	}
	otherRequestID, err := NewChannelRequestID(bytes.NewReader(bytesOf(0x62,
		channelRequestIDBytes)))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorTokenClosed})
	if err != nil {
		t.Fatal(err)
	}
	exact, err := NewChannelFrame(requestID, payload)
	if err != nil {
		t.Fatal(err)
	}
	received := receivedChannelFailure(requestID, exact)
	var failure *ChannelProtocolFailure
	if !stableChannelProtocolFailure(received) || !errors.As(received, &failure) ||
		failure.Code() != ChannelErrorTokenClosed {
		t.Fatalf("exact authenticated failure = %#v", received)
	}
	wrong, err := NewChannelFrame(otherRequestID, payload)
	if err != nil {
		t.Fatal(err)
	}
	received = receivedChannelFailure(requestID, wrong)
	if stableChannelProtocolFailure(received) || !errors.As(received, &failure) ||
		failure.Code() != ChannelErrorRosterConflict {
		t.Fatalf("wrong-request failure = %#v", received)
	}
}

func TestInstalledChannelJoinResultMustEqualAcceptedAuthority(t *testing.T) {
	fixture := testkit.NewSignedChannel(t, "peer-join-installed-invariant")
	roster, err := model.NewVerifiedRoster(fixture.Descriptor(),
		[]model.Member{fixture.OwnerMember().Member()})
	if err != nil {
		t.Fatal(err)
	}
	accepted := VerifiedChannelEnrollment{status: ChannelEnrollmentAccepted,
		descriptor: fixture.Descriptor(), roster: roster}
	result, err := NewChannelJoinResult(ChannelJoinResultSpec{Status: ChannelEnrollmentAccepted,
		Channel: fixture.Channel(), Roster: roster})
	if err != nil || !validInstalledChannelJoinResult(accepted, result,
		fixture.Channel().LocalAlias()) {
		t.Fatalf("matching installed authority = (%#v,%v)", result, err)
	}
	other := testkit.NewSignedChannel(t, "peer-join-installed-invariant-other")
	otherRoster, err := model.NewVerifiedRoster(other.Descriptor(),
		[]model.Member{other.OwnerMember().Member()})
	if err != nil {
		t.Fatal(err)
	}
	otherResult, err := NewChannelJoinResult(ChannelJoinResultSpec{Installed: true,
		Status:  ChannelEnrollmentAccepted,
		Channel: other.Channel(), Roster: otherRoster})
	if err != nil || validInstalledChannelJoinResult(accepted, otherResult,
		fixture.Channel().LocalAlias()) {
		t.Fatalf("cross-authority installed result = (%#v,%v)", otherResult, err)
	}
	original := fixture.Descriptor().Descriptor()
	altered, err := model.NewChannelDescriptor(model.ChannelDescriptorSpec{ID: original.ID(),
		Name: original.Name() + " altered", OwnerPeerID: original.OwnerPeerID(),
		OwnerPublicKey: original.OwnerPublicKey(), MemberLimit: original.MemberLimit(),
		CreatedAt: original.CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.ChannelDescriptorSigningMessage(altered.ID(), altered.Digest())
	if err != nil {
		t.Fatal(err)
	}
	signedAltered, err := model.AttachChannelDescriptorSignature(altered,
		ed25519.Sign(enrollmentPrivateKey(t, fixture.Owner()), message))
	if err != nil {
		t.Fatal(err)
	}
	alteredChannel, err := model.NewChannel(model.ChannelSpec{Descriptor: signedAltered,
		LocalAlias: fixture.Channel().LocalAlias(), RosterHead: roster.Head(),
		Status: model.ChannelActive, TopicState: model.TopicNotJoined,
		UpdatedAt: fixture.Channel().UpdatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	alteredResult, err := NewChannelJoinResult(ChannelJoinResultSpec{Installed: true,
		Status:  ChannelEnrollmentAccepted,
		Channel: alteredChannel, Roster: roster})
	if err != nil || validInstalledChannelJoinResult(accepted, alteredResult,
		fixture.Channel().LocalAlias()) {
		t.Fatalf("same-ID altered descriptor result = (%#v,%v)", alteredResult, err)
	}
}

func TestInstalledChannelJoinResultRejectsNonActiveAndForeignAliasProjection(t *testing.T) {
	fixture := testkit.NewSignedChannel(t, "peer-join-installed-state-invariant")
	roster, err := model.NewVerifiedRoster(fixture.Descriptor(),
		[]model.Member{fixture.OwnerMember().Member()})
	if err != nil {
		t.Fatal(err)
	}
	accepted := VerifiedChannelEnrollment{status: ChannelEnrollmentAccepted,
		descriptor: fixture.Descriptor(), roster: roster}
	installedReplay, err := NewChannelJoinResult(ChannelJoinResultSpec{Installed: true,
		Status: ChannelEnrollmentReplayed, Channel: fixture.Channel(), Roster: roster})
	if err != nil || validInstalledChannelJoinResult(accepted, installedReplay,
		fixture.Channel().LocalAlias()) {
		t.Fatalf("installed replay projection = (%#v,%v)", installedReplay, err)
	}
	for _, test := range []struct {
		name       string
		status     model.ChannelStatus
		topicState model.TopicState
		alias      string
	}{
		{name: "foreign_alias", status: model.ChannelActive, topicState: model.TopicNotJoined,
			alias: "foreign-alias"},
		{name: "conflicted", status: model.ChannelConflicted, topicState: model.TopicBlocked,
			alias: fixture.Channel().LocalAlias()},
		{name: "closed", status: model.ChannelClosed, topicState: model.TopicLeft,
			alias: fixture.Channel().LocalAlias()},
	} {
		t.Run(test.name, func(t *testing.T) {
			projected, buildErr := model.NewChannel(model.ChannelSpec{
				Descriptor: fixture.Descriptor(), LocalAlias: test.alias, RosterHead: roster.Head(),
				Status: test.status, TopicState: test.topicState, UpdatedAt: fixture.Channel().UpdatedAt(),
			})
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			result, resultErr := NewChannelJoinResult(ChannelJoinResultSpec{
				Status: ChannelEnrollmentAccepted, Channel: projected, Roster: roster})
			if resultErr != nil || validInstalledChannelJoinResult(accepted, result,
				fixture.Channel().LocalAlias()) {
				t.Fatalf("invalid installed projection = (%#v,%v)", result, resultErr)
			}
		})
	}
}

func TestInstalledChannelJoinResultPreservesTerminalNonInstallSemantics(t *testing.T) {
	for _, test := range []struct {
		name   string
		status ChannelEnrollmentStatus
		close  func(*testkit.SignedChannel)
	}{
		{name: "member_revoked", status: ChannelEnrollmentMemberRevoked,
			close: func(fixture *testkit.SignedChannel) {
				member := fixture.AppendActive(t, "peer-join-terminal-member")
				fixture.AppendTerminal(t, member.Identity().PeerID(), model.MemberRevoked)
			}},
		{name: "channel_closed", status: ChannelEnrollmentChannelClosed,
			close: func(fixture *testkit.SignedChannel) {
				fixture.AppendTerminal(t, fixture.Owner().PeerID(), model.MemberLeft)
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := testkit.NewSignedChannel(t, "peer-join-terminal-"+test.name)
			test.close(fixture)
			accepted := VerifiedChannelEnrollment{status: test.status,
				descriptor: fixture.Descriptor(), roster: fixture.Roster()}
			terminal, err := NewChannelJoinResult(ChannelJoinResultSpec{Status: test.status,
				Roster: fixture.Roster()})
			if err != nil || !validInstalledChannelJoinResult(accepted, terminal,
				fixture.Channel().LocalAlias()) {
				t.Fatalf("valid terminal result = (%#v,%v)", terminal, err)
			}
			installed, err := NewChannelJoinResult(ChannelJoinResultSpec{Installed: true,
				Status: test.status, Roster: fixture.Roster()})
			if err != nil || validInstalledChannelJoinResult(accepted, installed,
				fixture.Channel().LocalAlias()) {
				t.Fatalf("installed terminal result = (%#v,%v)", installed, err)
			}
			projected, err := NewChannelJoinResult(ChannelJoinResultSpec{Status: test.status,
				Channel: fixture.Channel(), Roster: fixture.Roster()})
			if err != nil || validInstalledChannelJoinResult(accepted, projected,
				fixture.Channel().LocalAlias()) {
				t.Fatalf("projected terminal Channel = (%#v,%v)", projected, err)
			}
		})
	}
}

func TestMeshRuntimeJoinChannelRetainsUnknownForPostMarkInvalidAcceptedEvidence(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(network.Stream, ChannelRequestID, model.RecordHead) error
	}{
		{name: "wrong_request", write: func(stream network.Stream, _ ChannelRequestID,
			_ model.RecordHead,
		) error {
			wrong, err := NewChannelRequestID(bytes.NewReader(bytesOf(0x71,
				channelRequestIDBytes)))
			if err != nil {
				return err
			}
			return writeChannelJoinTestFailure(stream, wrong, ChannelErrorTokenClosed)
		}},
		{name: "wrong_frame_type", write: func(stream network.Stream, requestID ChannelRequestID,
			head model.RecordHead,
		) error {
			return writeChannelJoinTestChallenge(stream, requestID, head)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newChannelJoinBindingFixture(t, "peer-join-post-mark-"+test.name)
			session := &recordingChannelJoinSession{epoch: fixture.joiner.OriginEpoch(),
				reserved: true}
			registerChannelJoinTestHandler(t, fixture.ctx, fixture.ownerHost,
				func(_ context.Context, stream network.Stream, frame ChannelFrame) error {
					if err := writeChannelJoinTestChallenge(stream, frame.RequestID(),
						fixture.channel.Channel().RosterHead()); err != nil {
						return err
					}
					proof, release, err := readChannelStreamFrame(stream, model.MaxChannelRecordBytes)
					if err != nil {
						return err
					}
					release()
					if proof.Type() != ChannelFrameEnrollProof {
						return errors.New("post-Mark fixture received non-proof frame")
					}
					return test.write(stream, frame.RequestID(),
						fixture.channel.Channel().RosterHead())
				})
			result, err := fixture.runtime.JoinChannel(fixture.ctx, fixture.spec, session)
			var failure *ChannelProtocolFailure
			if result.Status().Valid() || !errors.Is(err, ErrChannelEnrollmentOutcomeUnknown) ||
				errors.As(err, &failure) {
				t.Fatalf("post-Mark invalid accepted outcome = (%#v,%v)", result, err)
			}
			if begun, marked, released := session.counts(); begun != 1 || marked != 1 || released != 0 {
				t.Fatalf("post-Mark calls = begin %d, mark %d, release %d",
					begun, marked, released)
			}
		})
	}
}
