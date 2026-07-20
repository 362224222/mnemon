package localapi

import (
	"crypto/ed25519"
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

func validChannelContractView() ChannelView {
	invite := ChannelInviteView{ExpiresAt: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC).
		Format(time.RFC3339Nano), RemainingUses: 7, Status: "open"}
	return ChannelView{Alias: "alpha", Invite: &invite,
		Members:    []ChannelMemberView{{Alias: "self", Binding: "self", Reachability: "self", Status: "active"}},
		Membership: "active", Name: "Alpha", Owner: ChannelOwnerView{Local: true, Reachability: "self"},
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
