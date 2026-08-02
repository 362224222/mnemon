package peer

import (
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAuthoritySeparatesPhysicalAndExactChannelAccess(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "authority-local")
	remote := testAuthorityPeer(t, "authority-remote")
	authority, err := NewAuthority(local.modelID)
	if err != nil {
		t.Fatal(err)
	}
	alpha := testAuthorityChannel(t, "channel-alpha", model.BindingRevoked, local, remote)
	beta := testAuthorityChannel(t, "channel-beta", model.BindingPending, local, remote)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{alpha, beta}}); err != nil {
		t.Fatal(err)
	}
	alphaTopic, _ := TopicName(alpha.ChannelID)
	betaTopic, _ := TopicName(beta.ChannelID)
	if !authority.CanConnect(remote.libp2pID) {
		t.Fatal("pending authority in the overlapping Channel did not retain the physical connection")
	}
	if authority.CanOpenDataPlane(remote.libp2pID) {
		t.Fatal("pending binding gained Node data-plane stream authority")
	}
	if authority.CanUseTopic(remote.libp2pID, alphaTopic) ||
		authority.CanUseTopic(remote.libp2pID, betaTopic) {
		t.Fatal("revoked or pending binding gained topic access")
	}
	beta.Bindings[0].State = model.BindingActive
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{alpha, beta}}); err != nil {
		t.Fatal(err)
	}
	if !authority.CanUseTopic(remote.libp2pID, betaTopic) || !authority.CanUseTopic(local.libp2pID, betaTopic) ||
		authority.CanUseTopic(remote.libp2pID, alphaTopic) || authority.CanUseTopic(remote.libp2pID, betaTopic+"/") {
		t.Fatal("exact Channel topic authority was not enforced")
	}
	if !authority.CanOpenDataPlane(remote.libp2pID) {
		t.Fatal("active binding did not grant the shared Node data-plane stream")
	}
	if !authority.CanSubscribe(betaTopic) || authority.CanSubscribe(alphaTopic+"/") {
		t.Fatal("local subscription authority was not exact")
	}
	beta.Bindings[0].State = model.BindingRevoked
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{alpha, beta}}); err != nil {
		t.Fatal(err)
	}
	if authority.CanConnect(remote.libp2pID) || authority.CanOpenDataPlane(remote.libp2pID) {
		t.Fatal("peer remained physically or data-plane authorized after every Channel was revoked")
	}
}

func TestAuthorityPublicationCredentialUsesVerifiedPrefixAndCopiesKey(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "publication-local")
	remote := testAuthorityPeer(t, "publication-remote")
	authority, _ := NewAuthority(local.modelID)
	channel := testAuthorityChannel(t, "channel-publication", model.BindingActive, local, remote)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
		t.Fatal(err)
	}
	member := channel.Members[1]
	key, ok := authority.AuthorizePublication(channel.ChannelID, remote.libp2pID, remote.libp2pID,
		member.OriginEpoch, member.Head, channel.RosterHead)
	if !ok || string(key) != string(remote.publicKey) {
		t.Fatal("valid publication authority was not returned")
	}
	key[0] ^= 0xff
	reloaded, ok := authority.AuthorizePublication(channel.ChannelID, remote.libp2pID, remote.libp2pID,
		member.OriginEpoch, member.Head, channel.RosterHead)
	if !ok || string(reloaded) != string(remote.publicKey) {
		t.Fatal("publication authority exposed mutable key storage")
	}
	wrongEpoch, _ := model.ParseOriginEpoch("epoch-wrong")
	wrongHead, _ := model.NewRecordHead(3, model.Sum([]byte("unverified")))
	if _, ok := authority.AuthorizePublication(channel.ChannelID, remote.libp2pID, remote.libp2pID,
		wrongEpoch, member.Head, channel.RosterHead); ok {
		t.Fatal("wrong origin epoch was authorized")
	}
	if _, ok := authority.AuthorizePublication(channel.ChannelID, remote.libp2pID, remote.libp2pID,
		member.OriginEpoch, member.Head, wrongHead); ok {
		t.Fatal("unverified roster head was authorized")
	}
}

func TestAuthorityRuntimeRejectsUngatedReplacement(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "runtime-authority-local")
	remote := testAuthorityPeer(t, "runtime-authority-remote")
	authority, _ := NewAuthority(local.modelID)
	channel := testAuthorityChannel(t, "runtime-authority-channel", model.BindingActive, local, remote)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
		t.Fatal(err)
	}
	if !authority.bindRuntime() {
		t.Fatal("failed to bind the runtime authority owner")
	}
	revoked := channel
	revoked.Bindings = []BindingAuthoritySnapshot{{PeerID: remote.modelID, State: model.BindingRevoked}}
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{revoked}}); !errors.Is(err, ErrNetworkAuthority) {
		t.Fatalf("ungated runtime Replace() error = %v", err)
	}
	topic, _ := TopicName(channel.ChannelID)
	if !authority.CanUseTopic(remote.libp2pID, topic) {
		t.Fatal("rejected ungated replacement changed runtime authority")
	}
}

func TestAuthorityOutboundEnrollmentPermitDoesNotGrantChannelAccess(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "enrollment-permit-local")
	owner := testAuthorityPeer(t, "enrollment-permit-owner")
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		OutboundEnrollmentPeers: []model.PeerID{owner.modelID}}); err != nil {
		t.Fatal(err)
	}
	channelID, _ := model.ParseChannelID("not-installed")
	topic, _ := TopicName(channelID)
	if !authority.CanDial(owner.libp2pID) || authority.CanConnect(owner.libp2pID) ||
		authority.CanOpenDataPlane(owner.libp2pID) || authority.CanUseTopic(owner.libp2pID, topic) ||
		authority.CanSubscribe(topic) {
		t.Fatal("outbound enrollment permit leaked into durable Channel authority")
	}
}

func TestAuthorityKeepsAgencyAndR5ProtocolAuthorityIndependent(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "agency-authority-local")
	agencyOnly := testAuthorityPeer(t, "agency-authority-only")
	channelOnly := testAuthorityPeer(t, "agency-authority-channel")
	authority, _ := NewAuthority(local.modelID)
	channel := testAuthorityChannel(t, "agency-authority-r5", model.BindingActive, local, channelOnly)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels:    []ChannelAuthoritySnapshot{channel},
		AgencyPeers: []model.PeerID{agencyOnly.modelID}}); err != nil {
		t.Fatal(err)
	}
	if !authority.CanConnect(agencyOnly.libp2pID) || !authority.CanDial(agencyOnly.libp2pID) ||
		!authority.CanUseAgency(agencyOnly.libp2pID) ||
		authority.CanUseChannelControl(agencyOnly.libp2pID) ||
		authority.CanOpenDataPlane(agencyOnly.libp2pID) {
		t.Fatal("Agency-only Peer inherited R5 authority or lacked physical Agency authority")
	}
	if !authority.CanConnect(channelOnly.libp2pID) ||
		!authority.CanUseChannelControl(channelOnly.libp2pID) ||
		!authority.CanOpenDataPlane(channelOnly.libp2pID) ||
		authority.CanUseAgency(channelOnly.libp2pID) {
		t.Fatal("Channel-only Peer inherited Agency authority or lacked R5 authority")
	}
	gater := NewConnectionGater(authority)
	for _, protocolID := range []protocol.ID{GossipProtocol, ChannelProtocol,
		EventsProtocol, ArtifactsProtocol} {
		if gater.allowsProtocol(agencyOnly.libp2pID, protocolID, network.DirOutbound, "") {
			t.Fatalf("Agency-only Peer opened R5 protocol %s", protocolID)
		}
	}
	for _, protocolID := range []protocol.ID{AgencyDeliveryProtocol, AgencyObjectProtocol} {
		if !gater.allowsProtocol(agencyOnly.libp2pID, protocolID, network.DirOutbound, "") {
			t.Fatalf("Agency-only Peer could not open Agency protocol %s", protocolID)
		}
		if gater.allowsProtocol(channelOnly.libp2pID, protocolID, network.DirOutbound, "") {
			t.Fatalf("Channel-only Peer opened Agency protocol %s", protocolID)
		}
	}
}

func TestAuthorityRejectsInvalidAgencyPeerSnapshotAtomically(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "agency-authority-invalid-local")
	retained := testAuthorityPeer(t, "agency-authority-invalid-retained")
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		AgencyPeers: []model.PeerID{retained.modelID}}); err != nil {
		t.Fatal(err)
	}
	overLimit := make([]model.PeerID, maxAgencyPeers+1)
	for index := range overLimit {
		overLimit[index] = testAuthorityPeer(t,
			fmt.Sprintf("agency-authority-over-limit-%d", index)).modelID
	}
	invalid := []NetworkAuthoritySnapshot{
		{LocalPeerID: local.modelID, AgencyPeers: []model.PeerID{local.modelID}},
		{LocalPeerID: local.modelID, AgencyPeers: []model.PeerID{retained.modelID, retained.modelID}},
		{LocalPeerID: local.modelID, AgencyPeers: overLimit},
	}
	for _, snapshot := range invalid {
		if err := authority.Replace(snapshot); !errors.Is(err, ErrNetworkAuthority) {
			t.Fatalf("invalid Agency snapshot error = %v", err)
		}
		if !authority.CanUseAgency(retained.libp2pID) || !authority.CanConnect(retained.libp2pID) {
			t.Fatal("rejected Agency snapshot changed the current immutable authority")
		}
	}
}

func TestAuthorizePublicationNeverCombinesAuthorityRevisions(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "atomic-local")
	author := testAuthorityPeer(t, "atomic-author")
	relay := testAuthorityPeer(t, "atomic-relay")
	oldSnapshot := testThreePeerAuthorityChannel(t, "atomic-channel", local, author, relay)
	newHead, _ := model.NewRecordHead(4, model.Sum([]byte("atomic-new-head")))
	newSnapshot := oldSnapshot
	newSnapshot.VerifiedRosterHeads = append([]model.RecordHead(nil), oldSnapshot.VerifiedRosterHeads...)
	newSnapshot.VerifiedRosterHeads = append(newSnapshot.VerifiedRosterHeads, newHead)
	newSnapshot.RosterHead = newHead
	newSnapshot.Bindings = append([]BindingAuthoritySnapshot(nil), oldSnapshot.Bindings...)
	newSnapshot.Bindings[1].State = model.BindingRevoked
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{oldSnapshot}}); err != nil {
		t.Fatal(err)
	}
	authorMember := oldSnapshot.Members[1]
	var admitted atomic.Bool
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		for index := 0; index < 5_000; index++ {
			_ = authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
				Channels: []ChannelAuthoritySnapshot{newSnapshot}})
			_ = authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
				Channels: []ChannelAuthoritySnapshot{oldSnapshot}})
		}
	}()
	go func() {
		defer wait.Done()
		for index := 0; index < 20_000; index++ {
			if _, ok := authority.AuthorizePublication(oldSnapshot.ChannelID, relay.libp2pID,
				author.libp2pID, authorMember.OriginEpoch, authorMember.Head, newHead); ok {
				admitted.Store(true)
				return
			}
		}
	}()
	wait.Wait()
	if admitted.Load() {
		t.Fatal("publication combined old relay authority with a new roster revision")
	}
}

type authorityTestPeer struct {
	modelID       model.PeerID
	libp2pID      libp2ppeer.ID
	publicKey     ed25519.PublicKey
	privateKey    ed25519.PrivateKey
	libp2pPrivate libp2pcrypto.PrivKey
}

func testAuthorityPeer(t *testing.T, label string) authorityTestPeer {
	t.Helper()
	seed := sha256.Sum256([]byte(label))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	libp2pPrivate, err := libp2pcrypto.UnmarshalEd25519PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := libp2ppeer.IDFromPrivateKey(libp2pPrivate)
	if err != nil {
		t.Fatal(err)
	}
	modelID, err := model.ParsePeerID(peerID.String())
	if err != nil {
		t.Fatal(err)
	}
	return authorityTestPeer{modelID: modelID, libp2pID: peerID,
		publicKey:  append(ed25519.PublicKey(nil), privateKey.Public().(ed25519.PublicKey)...),
		privateKey: append(ed25519.PrivateKey(nil), privateKey...), libp2pPrivate: libp2pPrivate}
}

func testAuthorityChannel(t *testing.T, id string, binding model.BindingState,
	local, remote authorityTestPeer,
) ChannelAuthoritySnapshot {
	t.Helper()
	channelID, _ := model.ParseChannelID(id)
	localHead, _ := model.NewRecordHead(1, model.Sum([]byte(id+"-local")))
	remoteHead, _ := model.NewRecordHead(2, model.Sum([]byte(id+"-remote")))
	localEpoch, _ := model.ParseOriginEpoch("epoch-" + id + "-local")
	remoteEpoch, _ := model.ParseOriginEpoch("epoch-" + id + "-remote")
	return ChannelAuthoritySnapshot{ChannelID: channelID, Status: model.ChannelActive,
		RosterHead: remoteHead, VerifiedRosterHeads: []model.RecordHead{localHead, remoteHead},
		Bindings: []BindingAuthoritySnapshot{{PeerID: remote.modelID, State: binding}},
		Members: []MemberAuthoritySnapshot{
			{PeerID: local.modelID, OriginEpoch: localEpoch, Head: localHead, PublicKey: local.publicKey},
			{PeerID: remote.modelID, OriginEpoch: remoteEpoch, Head: remoteHead, PublicKey: remote.publicKey},
		}}
}
