package peer

import (
	"context"
	"crypto/ed25519"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestProjectNetworkAuthorityMapsLiveMeshAndPreservesRosterEvidence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	owner := testkit.NewIdentity(t, "peer-mesh-owner")
	st := openPeerMeshStore(t, owner, peerMeshTime(t, "2026-07-18T00:00:00Z"))

	active := testkit.NewSignedChannelForOwnerAt(t, "peer-mesh-active", owner,
		peerMeshTime(t, "2026-07-18T00:00:00Z"))
	createPeerMeshChannel(t, st, active, "active")
	formerActive := active.AppendActive(t, "peer-mesh-former")
	mergePeerMeshRoster(t, st, active, formerActive.Member(), formerActive.Member().CreatedAt())
	formerUpdate := active.AppendActiveUpdate(t, formerActive.Identity().PeerID())
	mergePeerMeshRoster(t, st, active, formerUpdate.Member(), formerUpdate.Member().CreatedAt())
	formerRevoked := active.AppendTerminal(t, formerActive.Identity().PeerID(), model.MemberRevoked)
	mergePeerMeshRoster(t, st, active, formerRevoked.Member(), formerRevoked.Member().CreatedAt())

	pending := testkit.NewSignedChannelForOwnerAt(t, "peer-mesh-pending", owner,
		peerMeshTime(t, "2026-07-18T01:00:00Z"))
	createPeerMeshChannel(t, st, pending, "pending")
	pendingRemote := pending.AppendActive(t, "peer-mesh-pending-remote")
	mergePeerMeshRoster(t, st, pending, pendingRemote.Member(), pendingRemote.Member().CreatedAt())

	conflicted := testkit.NewSignedChannelForOwnerAt(t, "peer-mesh-conflicted", owner,
		peerMeshTime(t, "2026-07-18T02:00:00Z"))
	createPeerMeshChannel(t, st, conflicted, "conflicted")
	incumbent := conflicted.AppendActive(t, "peer-mesh-incumbent")
	mergePeerMeshRoster(t, st, conflicted, incumbent.Member(), incumbent.Member().CreatedAt())
	challenger := peerMeshForkMember(t, conflicted, testkit.NewIdentity(t, "peer-mesh-challenger"),
		incumbent.Member().CreatedAt())
	result, err := st.MergeChannelRoster(ctx, store.MergeChannelRosterSpec{
		ChannelID: conflicted.Channel().ID(), AuthenticatedTransportPeerID: owner.PeerID(),
		Records: []model.Member{challenger}, At: incumbent.Member().CreatedAt().Add(time.Second),
	})
	if err != nil || result.Status != store.ChannelRosterConflicted {
		t.Fatalf("commit signed roster conflict = (%#v, %v)", result, err)
	}

	closed := testkit.NewSignedChannelForOwnerAt(t, "peer-mesh-closed", owner,
		peerMeshTime(t, "2026-07-18T03:00:00Z"))
	createPeerMeshChannel(t, st, closed, "closed")
	ownerLeft := closed.AppendTerminal(t, owner.PeerID(), model.MemberLeft)
	mergePeerMeshRoster(t, st, closed, ownerLeft.Member(), ownerLeft.Member().CreatedAt())

	mesh, err := st.ReadChannelMeshAuthority(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := ProjectNetworkAuthority(mesh)
	if err != nil {
		t.Fatal(err)
	}
	channels := peerMeshChannelsByID(snapshot.Channels)
	if len(channels) != 3 {
		t.Fatalf("runtime Channel count = %d, want three nonterminal Channels", len(channels))
	}
	if _, exists := channels[closed.Channel().ID()]; exists {
		t.Fatal("terminal Channel granted runtime authority")
	}
	activeAuthority := channels[active.Channel().ID()]
	if len(activeAuthority.VerifiedRosterHeads) != 4 || len(activeAuthority.Members) != 3 ||
		len(activeAuthority.Bindings) != 0 {
		t.Fatalf("revoked history projection = %#v", activeAuthority)
	}
	if activeAuthority.VerifiedRosterHeads[3] != formerRevoked.Member().Head() {
		t.Fatal("terminal roster evidence was not projected")
	}
	pendingAuthority := channels[pending.Channel().ID()]
	if len(pendingAuthority.Bindings) != 1 ||
		pendingAuthority.Bindings[0].State != model.BindingPending {
		t.Fatalf("pending binding projection = %#v", pendingAuthority.Bindings)
	}
	conflictAuthority := channels[conflicted.Channel().ID()]
	if conflictAuthority.Status != model.ChannelConflicted || len(conflictAuthority.Bindings) != 1 {
		t.Fatalf("conflicted projection = %#v", conflictAuthority)
	}

	runtime, err := NewAuthority(owner.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Replace(snapshot); err != nil {
		t.Fatal(err)
	}
	incumbentID, err := canonicalLibp2pID(incumbent.Identity().PeerID())
	if err != nil {
		t.Fatal(err)
	}
	conflictTopic, err := TopicName(conflicted.Channel().ID())
	if err != nil {
		t.Fatal(err)
	}
	if !runtime.CanConnect(incumbentID) || runtime.CanOpenDataPlane(incumbentID) ||
		runtime.CanSubscribe(conflictTopic) {
		t.Fatal("conflicted Channel did not retain physical/control-only authority")
	}

	firstKey := append([]byte(nil), activeAuthority.Members[0].PublicKey...)
	activeAuthority.Members[0].PublicKey[0] ^= 0xff
	reprojected, err := ProjectNetworkAuthority(mesh)
	if err != nil {
		t.Fatal(err)
	}
	if got := peerMeshChannelsByID(reprojected.Channels)[active.Channel().ID()].Members[0].PublicKey; !equalPeerMeshBytes(got, firstKey) {
		t.Fatal("network projection aliased Store member key storage")
	}
}

func TestProjectNetworkAuthorityRejectsIncompleteMeshSnapshot(t *testing.T) {
	t.Parallel()
	_, err := ProjectNetworkAuthority(store.ChannelMeshAuthority{})
	if !errors.Is(err, ErrNetworkAuthority) {
		t.Fatalf("incomplete mesh projection error = %v", err)
	}
}

func openPeerMeshStore(t *testing.T, identity testkit.Identity, at time.Time) *store.Store {
	t.Helper()
	workspace := t.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(workspace, "node", "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close peer mesh Store: %v", err)
		}
	})
	node, err := model.NewNode(model.NodeSpec{PeerID: identity.PeerID(),
		OriginEpoch: identity.OriginEpoch(), NextOriginSequence: 1,
		ActiveAssetRevision: "asset-r5-mesh", CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-peer-mesh", WorkspaceRoot: workspace, Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: model.Sum([]byte("peer-mesh-credential")),
		ActiveAssetRevision: "asset-r5-mesh", HandlingBudget: model.DefaultHandlingBudget().JSON(),
		Enabled: false, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
		t.Fatal(err)
	}
	return st
}

func createPeerMeshChannel(t *testing.T, st *store.Store, fixture *testkit.SignedChannel,
	seed string,
) {
	t.Helper()
	grantID, err := model.ParseGrantID("grant-peer-mesh-" + seed)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := model.NewEnrollmentTokenPayload(model.EnrollmentTokenSpec{
		Descriptor: fixture.Descriptor(), OwnerMultiaddrs: fixture.Owner().Multiaddrs(), GrantID: grantID,
		BearerSecret:       model.Sum([]byte("peer-mesh-secret:" + seed)).Bytes(),
		ExpiresAt:          fixture.Channel().CreatedAt().Add(time.Hour),
		MaxUses:            fixture.Channel().MemberLimit() - 1,
		ProtocolMinVersion: model.EnrollmentProtocolMinVersion,
		ProtocolMaxVersion: model.EnrollmentProtocolMaxVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.EnrollmentTokenSigningMessage(fixture.Channel().ID(), payload.Digest())
	if err != nil {
		t.Fatal(err)
	}
	privateKey := peerMeshPrivateKey(t, fixture.Owner())
	token, err := model.AttachEnrollmentTokenSignature(payload, ed25519.Sign(privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateChannel(context.Background(), store.CreateChannelSpec{
		Channel: fixture.Channel(), Genesis: fixture.OwnerMember().Member(), Token: token,
	}); err != nil {
		t.Fatalf("create peer mesh Channel: %v", err)
	}
}

func mergePeerMeshRoster(t *testing.T, st *store.Store, fixture *testkit.SignedChannel,
	record model.Member, at time.Time,
) {
	t.Helper()
	result, err := st.MergeChannelRoster(context.Background(), store.MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: fixture.Owner().PeerID(),
		Records: []model.Member{record}, At: at,
	})
	if err != nil || result.Status != store.ChannelRosterApplied {
		t.Fatalf("merge peer mesh roster = (%#v, %v)", result, err)
	}
}

func peerMeshForkMember(t *testing.T, fixture *testkit.SignedChannel, identity testkit.Identity,
	createdAt time.Time,
) model.Member {
	t.Helper()
	genesis := fixture.OwnerMember().Member()
	previous := genesis.Head().Digest()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{
		ChannelID: fixture.Channel().ID(), DescriptorDigest: fixture.Descriptor().Descriptor().Digest(),
		Revision: 2, PreviousDigest: &previous, PeerID: identity.PeerID(),
		OriginEpoch: identity.OriginEpoch(), DisplayLabel: identity.DisplayName(),
		PublicKey: identity.PublicKey(), Multiaddrs: identity.Multiaddrs(),
		Protocols: testkit.ChannelProtocols(), Limits: model.DefaultMemberLimits(),
		Status: model.MemberActive, CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	if err != nil {
		t.Fatal(err)
	}
	member, err := model.AttachMemberSignature(record,
		ed25519.Sign(peerMeshPrivateKey(t, fixture.Owner()), message))
	if err != nil {
		t.Fatal(err)
	}
	return member
}

func peerMeshPrivateKey(t testing.TB, identity testkit.Identity) ed25519.PrivateKey {
	t.Helper()
	key, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := key.Raw()
	if err != nil {
		t.Fatal(err)
	}
	return ed25519.PrivateKey(append([]byte(nil), raw...))
}

func peerMeshChannelsByID(channels []ChannelAuthoritySnapshot) map[model.ChannelID]ChannelAuthoritySnapshot {
	result := make(map[model.ChannelID]ChannelAuthoritySnapshot, len(channels))
	for _, channel := range channels {
		result[channel.ChannelID] = channel
	}
	return result
}

func peerMeshTime(t testing.TB, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func equalPeerMeshBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
