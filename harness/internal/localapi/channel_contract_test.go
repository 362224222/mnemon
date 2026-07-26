package localapi

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
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

func TestChannelJoinContractAcceptsReplayAndTerminalStatuses(t *testing.T) {
	t.Parallel()
	for _, status := range []string{"joined", "replayed", "member_revoked", "channel_closed"} {
		response := ChannelJoinResponse{SchemaVersion: SchemaVersion, Status: status,
			Channel: validChannelContractView()}
		if apiErr := validateChannelJoinResponse(response); apiErr != nil {
			t.Fatalf("join status %q rejected: %v", status, apiErr)
		}
	}
	response := ChannelJoinResponse{SchemaVersion: SchemaVersion, Status: "pending",
		Channel: validChannelContractView()}
	if validateChannelJoinResponse(response) == nil {
		t.Fatal("open join status passed the public contract")
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

func TestChannelStatusContractIsBoundedOperationalProjection(t *testing.T) {
	t.Parallel()
	channels := make([]ChannelView, model.MaxChannelsPerNode)
	for index := range channels {
		channels[index] = validChannelContractView()
		channels[index].Alias = fmt.Sprintf("alpha-%d", index)
	}
	status := ChannelStatusResponse{SchemaVersion: SchemaVersion, Status: "ok",
		Channels: channels}
	if apiErr := validateChannelStatusResponse(status); apiErr != nil {
		t.Fatalf("maximum bounded status rejected: %v", apiErr)
	}
	raw, err := model.CanonicalMarshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw)+1 > MaxChannelResponseBytes {
		t.Fatalf("maximum Channel status response = %d bytes, bound %d",
			len(raw)+1, MaxChannelResponseBytes)
	}
	if bytes.Contains(raw, []byte(`"publications"`)) {
		t.Fatalf("operational Channel status contains publication history: %s", raw)
	}
}

func validChannelContractView() ChannelView {
	const peerID = "12D3KooWCgPRroygp86pxPWqvQuXKSDf6CoJJHkmfEsNhm9rF46B"
	invite := ChannelInviteView{ExpiresAt: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC).
		Format(time.RFC3339Nano), RemainingUses: 7, Status: "open"}
	return ChannelView{Alias: "alpha", ChannelIDDigest: model.Sum([]byte("channel-alpha")).String(),
		Invite: &invite,
		Members: []ChannelMemberView{{Alias: "self", Binding: "self", PeerID: peerID,
			Reachability: "self", Status: "active"}},
		Membership: "active", Name: "Alpha", Owner: ChannelOwnerView{Local: true, Reachability: "self"},
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
