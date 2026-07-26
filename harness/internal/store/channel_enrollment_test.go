package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

type channelEnrollmentFixture struct {
	ownerStore *Store
	channel    *testkit.SignedChannel
	joiner     testkit.Identity
	grantID    model.GrantID
	requestID  model.EnrollmentRequestID
	token      model.EnrollmentToken
	signer     ChannelAuthoritySigner
	head       model.RecordHead
	acceptedAt time.Time
}

func newChannelEnrollmentFixture(t *testing.T, seed string) channelEnrollmentFixture {
	t.Helper()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, "enrollment-"+seed)
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
	grantID, _ := model.ParseGrantID("grant-" + seed)
	token := storeTestEnrollmentToken(t, channel.Descriptor(), channel.Owner(), grantID, seed,
		channel.Channel().CreatedAt(), model.MaxMembersPerChannel-1)
	if _, err := st.CreateChannel(context.Background(), CreateChannelSpec{Channel: channel.Channel(),
		Genesis: channel.OwnerMember().Member(), Token: token}); err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	joiner := testkit.NewIdentity(t, "joiner-"+seed)
	requestID := stableEnrollmentRequest(t, channel.Channel().ID(), grantID, joiner)
	signer := enrollmentTestSigner(t, channel.Owner())
	acceptedAt := channel.Channel().CreatedAt().Add(10 * time.Second)
	fixture := channelEnrollmentFixture{ownerStore: st, channel: channel, joiner: joiner,
		grantID: grantID, requestID: requestID, token: token, signer: signer,
		head: channel.Roster().Head(), acceptedAt: acceptedAt}
	prepared, err := st.PrepareChannelEnrollment(context.Background(), fixture.prepareSpec(acceptedAt))
	if err != nil || prepared.Status != ChannelEnrollmentAccepted || prepared.RosterHead != fixture.head {
		t.Fatalf("PrepareChannelEnrollment() = (%#v, %v)", prepared, err)
	}
	return fixture
}

func stableEnrollmentRequest(t testing.TB, channelID model.ChannelID, grantID model.GrantID,
	identity testkit.Identity,
) model.EnrollmentRequestID {
	t.Helper()
	joinIdentity, err := model.EnrollmentJoinIdentityDigest(channelID, grantID, identity.PeerID(),
		identity.PublicKey(), identity.OriginEpoch())
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := model.EnrollmentRequestIDForJoinIdentity(joinIdentity)
	if err != nil {
		t.Fatal(err)
	}
	return requestID
}

func reserveJoinedChannelTest(t testing.TB, st *Store, spec *InstallJoinedChannelSpec) {
	t.Helper()
	if spec == nil {
		t.Fatal("joined Channel install spec is nil")
	}
	prepared, err := st.PrepareJoinedChannel(context.Background(), PrepareJoinedChannelSpec{
		AuthenticatedLocalPeerID: spec.Transcript.JoinerPeerID(),
		LocalPublicKey:           spec.Transcript.JoinerPublicKey(),
		Descriptor:               spec.Descriptor,
		GrantID:                  spec.Transcript.GrantID(),
		LocalAlias:               spec.LocalAlias,
		At:                       spec.At,
	})
	if err != nil || prepared.RequestID != spec.Transcript.RequestID() ||
		prepared.OriginEpoch != spec.Transcript.JoinerOriginEpoch() || !prepared.Reserved {
		t.Fatalf("PrepareJoinedChannel() = (%#v, %v)", prepared, err)
	}
	if err := st.MarkJoinedChannelCommitUnknown(context.Background(), prepared.RequestID,
		spec.Transcript.JoinerPeerID(), prepared.Attempt, spec.At); err != nil {
		t.Fatalf("MarkJoinedChannelCommitUnknown() = %v", err)
	}
	spec.ReservationAttempt = prepared.Attempt
}

func (fixture channelEnrollmentFixture) prepareSpec(at time.Time) PrepareChannelEnrollmentSpec {
	return PrepareChannelEnrollmentSpec{ChannelID: fixture.channel.Channel().ID(), GrantID: fixture.grantID,
		RequestID: fixture.requestID, AuthenticatedPeerID: fixture.joiner.PeerID(),
		JoinerOriginEpoch: fixture.joiner.OriginEpoch(), JoinerPublicKey: fixture.joiner.PublicKey(), At: at}
}

func (fixture channelEnrollmentFixture) transcript(t testing.TB, ownerNonce, joinerNonce byte,
	head model.RecordHead,
) model.EnrollmentTranscript {
	t.Helper()
	return enrollmentTestTranscript(t, fixture.channel.Descriptor(), fixture.grantID,
		fixture.requestID, fixture.joiner, head, ownerNonce, joinerNonce)
}

func (fixture channelEnrollmentFixture) accept(t testing.TB,
	transcript model.EnrollmentTranscript,
) AcceptChannelEnrollmentResult {
	t.Helper()
	return fixture.acceptAt(t, transcript, fixture.acceptedAt)
}

func (fixture channelEnrollmentFixture) acceptAt(t testing.TB, transcript model.EnrollmentTranscript,
	at time.Time,
) AcceptChannelEnrollmentResult {
	t.Helper()
	proof := enrollmentTestProof(t, fixture.token, transcript)
	result, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
		AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(), Proof: proof, Signer: fixture.signer, At: at})
	if err != nil {
		t.Fatalf("AcceptChannelEnrollment() error = %v", err)
	}
	return result
}

func enrollmentTestTranscript(t testing.TB, descriptor model.SignedChannelDescriptor,
	grantID model.GrantID, requestID model.EnrollmentRequestID, joiner testkit.Identity,
	head model.RecordHead, ownerNonce, joinerNonce byte,
) model.EnrollmentTranscript {
	t.Helper()
	transcript, err := model.NewEnrollmentTranscript(model.EnrollmentTranscriptSpec{
		ChannelID: descriptor.Descriptor().ID(), GrantID: grantID, RequestID: requestID,
		OwnerPeerID: descriptor.Descriptor().OwnerPeerID(), JoinerPeerID: joiner.PeerID(),
		OwnerNonce:      bytes.Repeat([]byte{ownerNonce}, model.EnrollmentNonceBytes),
		JoinerNonce:     bytes.Repeat([]byte{joinerNonce}, model.EnrollmentNonceBytes),
		SelectedVersion: model.EnrollmentProtocolMinVersion, Limits: model.DefaultMemberLimits(),
		JoinerOriginEpoch: joiner.OriginEpoch(), JoinerDisplayLabel: joiner.DisplayName(),
		JoinerPublicKey: joiner.PublicKey(), AdvertisedMultiaddrs: joiner.Multiaddrs(), RosterHead: head})
	if err != nil {
		t.Fatal(err)
	}
	return transcript
}

func enrollmentTestProof(t testing.TB, token model.EnrollmentToken,
	transcript model.EnrollmentTranscript,
) model.Digest {
	t.Helper()
	verifier, err := model.VerifierForEnrollment(token.Payload().BearerSecret(), transcript.ChannelID(),
		transcript.GrantID())
	if err != nil {
		t.Fatal(err)
	}
	proof, err := model.ComputeEnrollmentProof(verifier, transcript)
	if err != nil {
		t.Fatal(err)
	}
	return proof
}

func enrollmentTestSigner(t testing.TB, identity testkit.Identity) ChannelAuthoritySigner {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := privateKey.Raw()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := eventpkg.NewEd25519Signer(ed25519.PrivateKey(raw))
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

type failAfterSigner struct {
	delegate  ChannelAuthoritySigner
	remaining int
}

func (signer *failAfterSigner) Sign(ctx context.Context, message []byte) ([]byte, error) {
	if signer.remaining == 0 {
		return nil, errors.New("injected signer failure")
	}
	signer.remaining--
	return signer.delegate.Sign(ctx, message)
}

func assertEnrollmentTableCounts(t testing.TB, st *Store, wants map[string]int) {
	t.Helper()
	for table, want := range wants {
		var got int
		if err := st.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s count = %d, err=%v, want %d", table, got, err, want)
		}
	}
}

func assertEnrollmentGrantProjection(t testing.TB, st *Store, grantID model.GrantID,
	wantStatus string, wantUsed uint8,
) {
	t.Helper()
	var status string
	var used uint8
	if err := st.db.QueryRow(`SELECT status,used_uses FROM enrollment_grants
		WHERE grant_id=?`, grantID.String()).Scan(&status, &used); err != nil ||
		status != wantStatus || used != wantUsed {
		t.Fatalf("grant projection = (%q,%d,%v), want (%q,%d)", status, used, err, wantStatus, wantUsed)
	}
}

// signAndAppendRosterMember signs one MemberRecord with the real owner
// authority, appends it, and re-verifies the complete roster.
func signAndAppendRosterMember(t testing.TB, descriptor model.SignedChannelDescriptor,
	ownerSigner ChannelAuthoritySigner, roster model.VerifiedRoster, spec model.MemberRecordSpec,
) (model.Member, model.VerifiedRoster) {
	t.Helper()
	record, err := model.NewMemberRecord(spec)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	signature, err := ownerSigner.Sign(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	member, err := model.AttachMemberSignature(record, signature)
	if err != nil {
		t.Fatal(err)
	}
	members := roster.Members()
	members = append(members, member)
	verified, err := model.NewVerifiedRoster(descriptor, members)
	if err != nil {
		t.Fatal(err)
	}
	return member, verified
}

// appendRosterMemberWithLabel creates a test-only label variant while
// preserving real owner signatures and complete roster verification.
func appendRosterMemberWithLabel(t testing.TB, descriptor model.SignedChannelDescriptor,
	ownerSigner ChannelAuthoritySigner, roster model.VerifiedRoster, identity testkit.Identity, label string,
) (model.Member, model.VerifiedRoster) {
	t.Helper()
	previous := roster.Head().Digest()
	members := roster.Members()
	return signAndAppendRosterMember(t, descriptor, ownerSigner, roster, model.MemberRecordSpec{
		ChannelID: descriptor.Descriptor().ID(), DescriptorDigest: descriptor.Descriptor().Digest(),
		Revision: roster.Head().Revision() + 1, PreviousDigest: &previous, PeerID: identity.PeerID(),
		OriginEpoch: identity.OriginEpoch(), DisplayLabel: label, PublicKey: identity.PublicKey(),
		Multiaddrs: identity.Multiaddrs(), Protocols: model.RequiredMemberProtocols(),
		Limits: model.DefaultMemberLimits(), Status: model.MemberActive,
		CreatedAt: members[len(members)-1].CreatedAt().Add(time.Second)})
}

func appendRosterTerminal(t testing.TB, descriptor model.SignedChannelDescriptor,
	ownerSigner ChannelAuthoritySigner, roster model.VerifiedRoster, peerID model.PeerID,
	status model.MemberStatus, at time.Time,
) (model.Member, model.VerifiedRoster) {
	t.Helper()
	current, ok := roster.CurrentMember(peerID)
	if !ok || current.Status() != model.MemberActive || !status.Terminal() {
		t.Fatal("terminal test member lacks active authority")
	}
	previous := roster.Head().Digest()
	return signAndAppendRosterMember(t, descriptor, ownerSigner, roster, model.MemberRecordSpec{
		ChannelID: descriptor.Descriptor().ID(), DescriptorDigest: descriptor.Descriptor().Digest(),
		Revision: roster.Head().Revision() + 1, PreviousDigest: &previous, PeerID: current.PeerID(),
		OriginEpoch: current.OriginEpoch(), DisplayLabel: current.DisplayLabel(),
		PublicKey: current.PublicKey(), Multiaddrs: current.Multiaddrs(),
		Protocols: current.Protocols(), Limits: current.Limits(), Status: status, CreatedAt: at})
}
