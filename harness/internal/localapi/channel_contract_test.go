package localapi

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelContractRejectsOpenShapesAndSecretsOutsideInviteResponses(t *testing.T) {
	channel := validChannelContractView()
	status := ChannelStatusResponse{SchemaVersion: SchemaVersion, Status: "ok",
		Channels: []ChannelView{channel}}
	if apiErr := validateChannelStatusResponse(status); apiErr != nil {
		t.Fatalf("valid status rejected: %v", apiErr)
	}
	status.Channels[0].Members[0].BaselineReady = true
	status.Channels[0].Members[0].Binding = "pending"
	if validateChannelStatusResponse(status) == nil {
		t.Fatal("pending member claimed baseline readiness")
	}
	if validChannelInviteRequest(ChannelInviteRequest{ExpiresSeconds: 299}) ||
		validChannelInviteRequest(ChannelInviteRequest{Uses: model.MaxMembersPerChannel}) ||
		validChannelInviteRequest(ChannelInviteRequest{Channel: "réview"}) ||
		validChannelCreateRequest(ChannelCreateRequest{Name: strings.Repeat("x", model.MaxLabelBytes+1)}) {
		t.Fatal("invalid Channel request passed the closed contract")
	}
	status = ChannelStatusResponse{SchemaVersion: SchemaVersion, Status: "ok",
		Channels: []ChannelView{validChannelContractView()}}
	status.Channels[0].Alias = "réview"
	if validateChannelStatusResponse(status) == nil {
		t.Fatal("non-ASCII Channel alias passed the public contract")
	}
}

func TestChannelAbandonContractIsClosedAndCanonical(t *testing.T) {
	t.Parallel()
	if !validChannelAbandonRequest(ChannelAbandonRequest{Channel: "alpha",
		ConfirmChannel: "alpha", Force: true}) {
		t.Fatal("exact destructive confirmation was rejected")
	}
	for _, request := range []ChannelAbandonRequest{
		{Channel: "alpha", ConfirmChannel: "alpha"},
		{Channel: "alpha", ConfirmChannel: "beta", Force: true},
		{Channel: "", ConfirmChannel: "", Force: true},
	} {
		if validChannelAbandonRequest(request) {
			t.Fatalf("unsafe abandon request accepted: %#v", request)
		}
	}
	valid := ChannelAbandonResponse{SchemaVersion: SchemaVersion, Status: "abandoned",
		Channel: "alpha", TransitionedAt: "2026-07-21T10:00:00.123456789Z",
		Evidence: ChannelForensicCounts{Bindings: 2, Events: 7}}
	if apiErr := validateChannelAbandonResponse(valid); apiErr != nil {
		t.Fatalf("valid abandon response rejected: %v", apiErr)
	}
	valid.TransitionedAt = "2026-07-21T10:00:00.1234567890Z"
	if validateChannelAbandonResponse(valid) == nil {
		t.Fatal("noncanonical transition time accepted")
	}
}

func TestChannelStatusContractClosesEvidencePathSemantics(t *testing.T) {
	t.Parallel()
	const (
		origin = "12D3KooWCgPRroygp86pxPWqvQuXKSDf6CoJJHkmfEsNhm9rF46B"
		target = "12D3KooWLzW3XvRNG5Jv84reMiXzrU1QpkwQCrw4EP8AVSv4GDKJ"
	)
	channel := validChannelContractView()
	channel.Publications = []ChannelPublicationView{{
		Arrival: "local", ArtifactDirectSourcePeerID: nil, AudiencePeerIDs: []string{target},
		CausalityEventKey: nil, ChannelIDDigest: channel.ChannelIDDigest,
		EventDigest: model.Sum([]byte("event-alpha")).String(),
		EventKey: ChannelEventKeyView{EventID: "event-alpha", OriginEpoch: "epoch-alpha",
			OriginPeerID: origin},
		IgnoredPeerIDs: []string{}, ImmediateTransportPeerID: origin, OriginPeerID: origin,
		PublicationDigest: model.Sum([]byte("publication-alpha")).String(),
		PublicationRef: ChannelPublicationRefView{ChannelSequence: 1,
			OriginEpoch: "epoch-alpha", OriginPeerID: origin},
		SemanticOutcome: "originated",
	}}
	status := ChannelStatusResponse{SchemaVersion: SchemaVersion, Status: "ok",
		Channels: []ChannelView{channel}}
	if apiErr := validateChannelStatusResponse(status); apiErr != nil {
		t.Fatalf("valid evidence status rejected: %v", apiErr)
	}

	wrongSource := status
	wrongSource.Channels = append([]ChannelView(nil), status.Channels...)
	wrongSource.Channels[0].Publications = append([]ChannelPublicationView(nil), channel.Publications...)
	wrongSource.Channels[0].Publications[0].ArtifactDirectSourcePeerID = pointerToString(target)
	if validateChannelStatusResponse(wrongSource) == nil {
		t.Fatal("Artifact direct source different from signed origin passed")
	}
	wrongTransport := status
	wrongTransport.Channels = append([]ChannelView(nil), status.Channels...)
	wrongTransport.Channels[0].Publications = append([]ChannelPublicationView(nil), channel.Publications...)
	wrongTransport.Channels[0].Publications[0].ImmediateTransportPeerID = target
	if validateChannelStatusResponse(wrongTransport) == nil {
		t.Fatal("local publication with a relay transport passed")
	}
	wrongRef := status
	wrongRef.Channels = append([]ChannelView(nil), status.Channels...)
	wrongRef.Channels[0].Publications = append([]ChannelPublicationView(nil), channel.Publications...)
	wrongRef.Channels[0].Publications[0].PublicationRef.ChannelSequence = 0
	if validateChannelStatusResponse(wrongRef) == nil {
		t.Fatal("zero-sequence publication reference passed")
	}
}

func pointerToString(value string) *string { return &value }

func validChannelContractView() ChannelView {
	const peerID = "12D3KooWCgPRroygp86pxPWqvQuXKSDf6CoJJHkmfEsNhm9rF46B"
	invite := ChannelInviteView{ExpiresAt: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC).
		Format(time.RFC3339Nano), RemainingUses: 7, Status: "open"}
	return ChannelView{Alias: "alpha", ChannelIDDigest: model.Sum([]byte("channel-alpha")).String(),
		Invite: &invite,
		Members: []ChannelMemberView{{Alias: "self", Binding: "self", PeerID: peerID,
			Reachability: "self", Status: "active"}},
		Membership: "active", Name: "Alpha", Owner: ChannelOwnerView{Local: true, Reachability: "self"},
		Publications: []ChannelPublicationView{},
		RosterHead: ChannelRosterHeadView{Digest: model.Sum([]byte("roster-alpha")).String(),
			OwnerPeerID: peerID, OwnerSignature: base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
			Revision: 1},
		RosterRevision: 1, Topic: ChannelTopicView{Status: "converging", TotalMembers: 1}}
}

func validChannelContractToken(t *testing.T) string {
	t.Helper()
	fixture := testkit.NewSignedChannel(t, "localapi-channel-contract")
	grantID, _ := model.ParseGrantID("grant-localapi-channel-contract")
	secret := make([]byte, model.EnrollmentSecretBytes)
	for index := range secret {
		secret[index] = byte(index + 1)
	}
	payload, err := model.NewEnrollmentTokenPayload(model.EnrollmentTokenSpec{
		Descriptor: fixture.Descriptor(), OwnerMultiaddrs: fixture.Owner().Multiaddrs(), GrantID: grantID,
		BearerSecret: secret, ExpiresAt: fixture.Channel().CreatedAt().Add(time.Hour), MaxUses: 7,
		ProtocolMinVersion: model.EnrollmentProtocolMinVersion,
		ProtocolMaxVersion: model.EnrollmentProtocolMaxVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.EnrollmentTokenSigningMessage(fixture.Channel().ID(), payload.Digest())
	privateKey, err := fixture.Owner().Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	rawPrivate, err := privateKey.Raw()
	if err != nil {
		t.Fatal(err)
	}
	token, err := model.AttachEnrollmentTokenSignature(payload, ed25519.Sign(rawPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	return token.Reveal()
}
