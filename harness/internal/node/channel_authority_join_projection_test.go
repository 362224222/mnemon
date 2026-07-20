package node

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelJoinProjectionBindsActiveAuthorityAndInstallSemantics(t *testing.T) {
	evidence, _ := newChannelJoinProjectionEvidence(t, "active")
	joined := joinedChannelProjection(t, evidence, "projection-team", model.ChannelActive)
	result := store.InstallJoinedChannelResult{Installed: true,
		Status: store.ChannelEnrollmentAccepted, Channel: joined, Roster: evidence.roster}
	projected, err := projectChannelJoinResult(evidence, "projection-team", result)
	if err != nil || !projected.Installed() ||
		projected.Status() != peer.ChannelEnrollmentAccepted ||
		projected.Channel().LocalAlias() != "projection-team" ||
		!sameJoinedChannelRoster(projected.Roster(), evidence.roster) {
		t.Fatalf("active projection = (%#v,%v)", projected, err)
	}

	expected := result
	expected.Installed = false
	if !sameJoinedChannelInstallResult(result, expected, true) ||
		sameJoinedChannelInstallResult(result, expected, false) {
		t.Fatal("fresh install flag was not bound to an authority-changing plan")
	}
	replayedEvidence := evidence
	replayedEvidence.status = peer.ChannelEnrollmentReplayed
	if recovered, err := projectChannelJoinResult(replayedEvidence, "projection-team", result); err != nil ||
		!recovered.Installed() || recovered.Status() != peer.ChannelEnrollmentAccepted {
		t.Fatalf("owner replay without local replica = (%#v,%v)", recovered, err)
	}
	localReplay := result
	localReplay.Installed = false
	localReplay.Status = store.ChannelEnrollmentReplayed
	if projected, err := projectChannelJoinResult(evidence, "projection-team", localReplay); err == nil ||
		projected.Status().Valid() {
		t.Fatalf("owner accepted with local replay = (%#v,%v)", projected, err)
	}
	wrongStatus := result
	wrongStatus.Status = store.ChannelEnrollmentReplayed
	wrongAlias := result
	wrongAlias.Channel = joinedChannelProjection(t, evidence, "other-team", model.ChannelActive)
	wrongRoster := result
	wrongRoster.Roster = model.VerifiedRoster{}
	for name, candidate := range map[string]store.InstallJoinedChannelResult{
		"status": wrongStatus, "alias": wrongAlias, "roster": wrongRoster,
	} {
		if projected, err := projectChannelJoinResult(evidence, "projection-team", candidate); err == nil ||
			projected.Status().Valid() {
			t.Fatalf("mismatched %s projection = (%#v,%v)", name, projected, err)
		}
	}
}

func TestChannelJoinProjectionHidesExistingTerminalReplica(t *testing.T) {
	evidence, _ := newChannelJoinProjectionEvidence(t, "terminal")
	evidence.status = peer.ChannelEnrollmentMemberRevoked
	owner := testkit.NewIdentity(t, "node-owner-enrollment-join-projection-terminal")
	evidence.roster = appendTerminalJoinMember(t, evidence.descriptor, evidence.roster,
		owner, evidence.transcript.JoinerPeerID(), model.MemberRevoked)
	terminal := joinedChannelProjection(t, evidence, "terminal-team", model.ChannelLeft)
	result := store.InstallJoinedChannelResult{Status: store.ChannelEnrollmentMemberRevoked,
		Channel: terminal, Roster: evidence.roster}
	projected, err := projectChannelJoinResult(evidence, "terminal-team", result)
	if err != nil || projected.Installed() || !projected.Channel().ID().IsZero() ||
		projected.Status() != peer.ChannelEnrollmentMemberRevoked ||
		projected.Roster().Head() != evidence.roster.Head() {
		t.Fatalf("terminal projection = (%#v,%v)", projected, err)
	}
	closed := result
	closed.Channel = joinedChannelProjection(t, evidence, "terminal-team", model.ChannelClosed)
	projected, err = projectChannelJoinResult(evidence, "terminal-team", closed)
	if err != nil || !projected.Channel().ID().IsZero() ||
		projected.Status() != peer.ChannelEnrollmentMemberRevoked {
		t.Fatalf("revoked member on owner-closed replica = (%#v,%v)", projected, err)
	}
	result.Installed = true
	if projected, err := projectChannelJoinResult(evidence, "terminal-team", result); err == nil ||
		projected.Status().Valid() {
		t.Fatalf("installed terminal projection = (%#v,%v)", projected, err)
	}
}

func newChannelJoinProjectionEvidence(t *testing.T, seed string) (
	frozenChannelJoinEvidence, store.AcceptChannelEnrollmentResult,
) {
	t.Helper()
	fixture := newChannelAuthorityEnrollmentFixture(t, "join-projection-"+seed)
	result, err := fixture.store.AcceptChannelEnrollment(context.Background(),
		store.AcceptChannelEnrollmentSpec{AuthenticatedPeerID: fixture.acceptance.AuthenticatedPeerID,
			Transcript:           fixture.acceptance.Transcript,
			AdvertisedMultiaddrs: fixture.acceptance.AdvertisedMultiaddrs,
			Proof:                fixture.acceptance.Proof, Signer: newChannelAuthorityTestSigner(t, fixture.owner),
			At: fixture.acceptance.At})
	if err != nil {
		t.Fatal(err)
	}
	return frozenChannelJoinEvidence{owner: fixture.owner.PeerID(),
		status: peer.ChannelEnrollmentAccepted, descriptor: result.Channel.Descriptor(),
		transcript: fixture.acceptance.Transcript, receipt: result.Receipt,
		roster: result.Roster}, result
}

func joinedChannelProjection(t *testing.T, evidence frozenChannelJoinEvidence,
	alias string, status model.ChannelStatus,
) model.Channel {
	t.Helper()
	topic := model.TopicNotJoined
	if status.Terminal() {
		topic = model.TopicLeft
	}
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: evidence.descriptor,
		LocalAlias: alias, RosterHead: evidence.roster.Head(), Status: status,
		TopicState: topic, UpdatedAt: evidence.roster.Members()[len(evidence.roster.Members())-1].CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	return channel
}

func appendTerminalJoinMember(t *testing.T, descriptor model.SignedChannelDescriptor,
	roster model.VerifiedRoster, owner testkit.Identity, peerID model.PeerID,
	status model.MemberStatus,
) model.VerifiedRoster {
	t.Helper()
	current, ok := roster.CurrentMember(peerID)
	if !ok {
		t.Fatal("terminal projection member is absent")
	}
	previous := roster.Head().Digest()
	members := roster.Members()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{
		ChannelID:        descriptor.Descriptor().ID(),
		DescriptorDigest: descriptor.Descriptor().Digest(),
		Revision:         roster.Head().Revision() + 1, PreviousDigest: &previous,
		PeerID: current.PeerID(), OriginEpoch: current.OriginEpoch(),
		DisplayLabel: current.DisplayLabel(), PublicKey: current.PublicKey(),
		Multiaddrs: current.Multiaddrs(), Protocols: current.Protocols(), Limits: current.Limits(),
		Status: status, CreatedAt: members[len(members)-1].CreatedAt().Add(time.Nanosecond)})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := owner.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := privateKey.Raw()
	if err != nil || owner.PeerID() != descriptor.Descriptor().OwnerPeerID() {
		t.Fatalf("terminal owner identity mismatch: %v", err)
	}
	member, err := model.AttachMemberSignature(record,
		ed25519.Sign(ed25519.PrivateKey(raw), message))
	if err != nil {
		t.Fatal(err)
	}
	members = append(members, member)
	updated, err := model.NewVerifiedRoster(descriptor, members)
	if err != nil {
		t.Fatal(err)
	}
	return updated
}
