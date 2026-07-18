package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestArtifactSourceAuthorizesEventAudienceAndFrozenWorkParticipant(t *testing.T) {
	t.Parallel()
	fixture := newArtifactSourceFixture(t)
	ctx := context.Background()

	for name, requester := range map[string]model.PeerID{
		"immutable Event audience": fixture.audience.Identity().PeerID(),
		"frozen Work participant":  fixture.participant.Identity().PeerID(),
	} {
		t.Run(name, func(t *testing.T) {
			manifest, err := fixture.store.ReadArtifactSourceManifest(ctx,
				ReadArtifactSourceManifestSpec{AuthenticatedPeerID: requester,
					ChannelID: fixture.channel.Channel().ID(), RootDigest: fixture.root.RootDigest})
			if err != nil || manifest.RootDigest() != fixture.root.RootDigest ||
				manifest.ManifestDigest() != fixture.root.ManifestDigest ||
				manifest.TotalBytes() != fixture.root.TotalBytes ||
				!bytes.Equal(manifest.ManifestBytes(), fixture.root.Manifest.Bytes()) {
				t.Fatalf("ReadArtifactSourceManifest() = (%#v, %v)", manifest, err)
			}
			block, err := fixture.store.ReadArtifactSourceBlock(ctx,
				ReadArtifactSourceBlockSpec{AuthenticatedPeerID: requester,
					ChannelID: fixture.channel.Channel().ID(), RootDigest: fixture.root.RootDigest,
					BlockDigest: fixture.block.Digest})
			if err != nil || block.RootDigest() != fixture.root.RootDigest ||
				block.BlockDigest() != fixture.block.Digest || block.SizeBytes() != fixture.block.SizeBytes {
				t.Fatalf("ReadArtifactSourceBlock() = (%#v, %v)", block, err)
			}

			first := manifest.ManifestBytes()
			first[0] = 'x'
			second := manifest.Manifest().Bytes()
			second[len(second)-1] = 'x'
			if !bytes.Equal(manifest.ManifestBytes(), fixture.root.Manifest.Bytes()) {
				t.Fatal("Artifact source manifest exposed an aliased byte slice")
			}
		})
	}

	// Effective aliases are mutable local UX projections. Frozen Work authority
	// remains bound to the exact immutable PeerID snapshot.
	mustExec(t, fixture.store, `UPDATE peer_bindings SET effective_alias=?
		WHERE channel_id=? AND peer_id=?`, "renamed-participant",
		fixture.channel.Channel().ID().String(), fixture.participant.Identity().PeerID().String())
	if _, err := fixture.store.ReadArtifactSourceManifest(ctx,
		ReadArtifactSourceManifestSpec{AuthenticatedPeerID: fixture.participant.Identity().PeerID(),
			ChannelID: fixture.channel.Channel().ID(), RootDigest: fixture.root.RootDigest}); err != nil {
		t.Fatalf("frozen participant after alias change: %v", err)
	}
}

func TestArtifactSourceClosesUnknownMembershipAndClosureDenials(t *testing.T) {
	t.Parallel()
	fixture := newArtifactSourceFixture(t)
	ctx := context.Background()
	channelID := fixture.channel.Channel().ID()
	unknown, _ := model.ParsePeerID("peer-artifact-source-unknown")
	unknownChannel, _ := model.ParseChannelID("channel-artifact-source-unknown")
	unknownRoot := model.Sum([]byte("artifact-source-unknown-root"))

	manifestCases := []struct {
		name      string
		requester model.PeerID
		channel   model.ChannelID
		root      model.Digest
	}{
		{name: "unknown requester", requester: unknown, channel: channelID, root: fixture.root.RootDigest},
		{name: "active nonparticipant", requester: fixture.observer.Identity().PeerID(), channel: channelID, root: fixture.root.RootDigest},
		{name: "pending binding", requester: fixture.pending.Identity().PeerID(), channel: channelID, root: fixture.root.RootDigest},
		{name: "revoked member", requester: fixture.revoked.Identity().PeerID(), channel: channelID, root: fixture.root.RootDigest},
		{name: "unknown Channel", requester: fixture.participant.Identity().PeerID(), channel: unknownChannel, root: fixture.root.RootDigest},
		{name: "known root without Event pin", requester: fixture.participant.Identity().PeerID(), channel: channelID, root: fixture.otherRoot.RootDigest},
		{name: "unknown root", requester: fixture.participant.Identity().PeerID(), channel: channelID, root: unknownRoot},
	}
	for _, test := range manifestCases {
		t.Run(test.name, func(t *testing.T) {
			_, err := fixture.store.ReadArtifactSourceManifest(ctx,
				ReadArtifactSourceManifestSpec{AuthenticatedPeerID: test.requester,
					ChannelID: test.channel, RootDigest: test.root})
			assertArtifactSourceUnavailable(t, err)
		})
	}

	_, err := fixture.store.ReadArtifactSourceBlock(ctx,
		ReadArtifactSourceBlockSpec{AuthenticatedPeerID: fixture.participant.Identity().PeerID(),
			ChannelID: channelID, RootDigest: fixture.root.RootDigest,
			BlockDigest: fixture.otherBlock.Digest})
	assertArtifactSourceUnavailable(t, err)
	_, err = fixture.store.ReadArtifactSourceBlock(ctx,
		ReadArtifactSourceBlockSpec{AuthenticatedPeerID: fixture.observer.Identity().PeerID(),
			ChannelID: channelID, RootDigest: fixture.root.RootDigest,
			BlockDigest: fixture.block.Digest})
	assertArtifactSourceUnavailable(t, err)

	// The requester is active in both Channels, but the root has an immutable
	// Event pin only in the first one. Knowing the digest cannot bridge scope.
	cross := testkit.NewSignedChannelForOwnerAt(t, "artifact-source-cross", fixture.channel.Owner(),
		fixture.channel.Channel().UpdatedAt().Add(time.Hour))
	crossMember := cross.AppendActiveIdentity(t, fixture.participant.Identity())
	insertSignedChannelFixture(t, fixture.store.db, cross, model.TopicJoined)
	insertSignedPeerBinding(t, fixture.store.db, cross.Channel().ID(), crossMember, "cross-participant",
		model.BindingPending, model.ReachabilityUnknown, cross.Channel().UpdatedAt())
	insertPeerCursorForMeshTest(t, fixture.store, cross.Channel().ID(), crossMember)
	mustExec(t, fixture.store, `UPDATE peer_bindings SET state='active'
		WHERE channel_id=? AND peer_id=?`, cross.Channel().ID().String(),
		fixture.participant.Identity().PeerID().String())
	_, err = fixture.store.ReadArtifactSourceManifest(ctx,
		ReadArtifactSourceManifestSpec{AuthenticatedPeerID: fixture.participant.Identity().PeerID(),
			ChannelID: cross.Channel().ID(), RootDigest: fixture.root.RootDigest})
	assertArtifactSourceUnavailable(t, err)

	t.Run("non-active Channel", func(t *testing.T) {
		paused := newArtifactSourceFixture(t)
		mustExec(t, paused.store, `UPDATE channels SET status='leaving',topic_state='left' WHERE channel_id=?`,
			paused.channel.Channel().ID().String())
		_, err := paused.store.ReadArtifactSourceManifest(ctx,
			ReadArtifactSourceManifestSpec{AuthenticatedPeerID: paused.participant.Identity().PeerID(),
				ChannelID: paused.channel.Channel().ID(), RootDigest: paused.root.RootDigest})
		assertArtifactSourceUnavailable(t, err)
	})
}

func TestArtifactSourceRejectsIncompleteInput(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	peer, _ := model.ParsePeerID("peer-artifact-source-input")
	channel, _ := model.ParseChannelID("channel-artifact-source-input")
	root := model.Sum([]byte("artifact-source-input"))

	if _, err := st.ReadArtifactSourceManifest(context.Background(),
		ReadArtifactSourceManifestSpec{AuthenticatedPeerID: peer, ChannelID: channel}); !errors.Is(err, ErrArtifactSourceInput) {
		t.Fatalf("incomplete manifest source error = %v", err)
	}
	if _, err := st.ReadArtifactSourceBlock(context.Background(),
		ReadArtifactSourceBlockSpec{AuthenticatedPeerID: peer, ChannelID: channel,
			RootDigest: root}); !errors.Is(err, ErrArtifactSourceInput) {
		t.Fatalf("incomplete block source error = %v", err)
	}
}

func TestArtifactSourceFailsClosedOnDurableCorruption(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*testing.T, *artifactSourceFixture)
	}{
		{
			name: "Channel authority projection",
			mutate: func(t *testing.T, fixture *artifactSourceFixture) {
				mustExec(t, fixture.store, `DROP TRIGGER channels_descriptor_immutable`)
				mustExec(t, fixture.store, `UPDATE channels SET name='forged Channel'
					WHERE channel_id=?`, fixture.channel.Channel().ID().String())
			},
		},
		{
			name: "canonical Event bytes",
			mutate: func(t *testing.T, fixture *artifactSourceFixture) {
				mustExec(t, fixture.store, `DROP TRIGGER events_no_update`)
				mustExec(t, fixture.store, `UPDATE events SET canonical_event_json='{}'
					WHERE event_id=?`, fixture.event.ID().String())
			},
		},
		{
			name: "Event origin signature",
			mutate: func(t *testing.T, fixture *artifactSourceFixture) {
				mustExec(t, fixture.store, `DROP TRIGGER events_no_update`)
				mustExec(t, fixture.store, `UPDATE events SET origin_signature=? WHERE event_id=?`,
					bytes.Repeat([]byte{0xff}, ed25519.SignatureSize), fixture.event.ID().String())
			},
		},
		{
			name: "Event audience projection",
			mutate: func(t *testing.T, fixture *artifactSourceFixture) {
				mustExec(t, fixture.store, `DROP TRIGGER events_no_update`)
				replacement, _ := model.JSONFrom([]model.PeerID{fixture.observer.Identity().PeerID()})
				mustExec(t, fixture.store, `UPDATE events SET audience_json=? WHERE event_id=?`,
					replacement.Bytes(), fixture.event.ID().String())
			},
		},
		{
			name: "canonical manifest",
			mutate: func(t *testing.T, fixture *artifactSourceFixture) {
				mustExec(t, fixture.store, `DROP TRIGGER artifact_roots_content_immutable`)
				mustExec(t, fixture.store, `UPDATE artifact_roots SET manifest_json=? WHERE root_digest=?`,
					[]byte(" {\"not\":\"exact\"}"), fixture.root.RootDigest.String())
			},
		},
		{
			name: "root digest binding",
			mutate: func(t *testing.T, fixture *artifactSourceFixture) {
				mustExec(t, fixture.store, `DROP TRIGGER artifact_roots_content_immutable`)
				mustExec(t, fixture.store, `UPDATE artifact_roots SET manifest_json=?,manifest_digest=?,total_bytes=?
					WHERE root_digest=?`, fixture.otherRoot.Manifest.Bytes(),
					fixture.otherRoot.ManifestDigest.Bytes(), fixture.otherRoot.TotalBytes,
					fixture.root.RootDigest.String())
			},
		},
		{
			name: "sealed root block map",
			mutate: func(t *testing.T, fixture *artifactSourceFixture) {
				mustExec(t, fixture.store, `DROP TRIGGER artifact_root_blocks_no_update`)
				mustExec(t, fixture.store, `UPDATE artifact_root_blocks SET logical_path='forged.txt'
					WHERE root_digest=?`, fixture.root.RootDigest.String())
			},
		},
		{
			name: "reachable block metadata",
			mutate: func(t *testing.T, fixture *artifactSourceFixture) {
				mustExec(t, fixture.store, `DROP TRIGGER artifact_blocks_no_update`)
				mustExec(t, fixture.store, `UPDATE artifact_blocks SET size_bytes=size_bytes+1
					WHERE block_digest=?`, fixture.block.Digest.String())
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newArtifactSourceFixture(t)
			test.mutate(t, fixture)
			_, err := fixture.store.ReadArtifactSourceManifest(context.Background(),
				ReadArtifactSourceManifestSpec{AuthenticatedPeerID: fixture.participant.Identity().PeerID(),
					ChannelID: fixture.channel.Channel().ID(), RootDigest: fixture.root.RootDigest})
			if !errors.Is(err, ErrArtifactSourceInvariant) ||
				errors.Is(err, ErrArtifactSourceUnavailable) {
				t.Fatalf("corrupt Artifact source error = %v", err)
			}
		})
	}
}

type artifactSourceFixture struct {
	store       *Store
	channel     *testkit.SignedChannel
	participant testkit.MemberFixture
	audience    testkit.MemberFixture
	observer    testkit.MemberFixture
	pending     testkit.MemberFixture
	revoked     testkit.MemberFixture
	root        VerifiedArtifactRoot
	block       VerifiedArtifactBlock
	otherRoot   VerifiedArtifactRoot
	otherBlock  VerifiedArtifactBlock
	event       model.Event
}

func newArtifactSourceFixture(t *testing.T) *artifactSourceFixture {
	t.Helper()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, "artifact-source-"+t.Name())
	participant := channel.AppendActive(t, "artifact-source-participant-"+t.Name())
	audience := channel.AppendActive(t, "artifact-source-audience-"+t.Name())
	observer := channel.AppendActive(t, "artifact-source-observer-"+t.Name())
	pending := channel.AppendActive(t, "artifact-source-pending-"+t.Name())
	revokedActive := channel.AppendActive(t, "artifact-source-revoked-"+t.Name())
	revoked := channel.AppendTerminal(t, revokedActive.Identity().PeerID(), model.MemberRevoked)
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt().Add(-time.Second))
	insertSignedChannelFixture(t, st.db, channel, model.TopicJoined)

	for index, member := range []testkit.MemberFixture{participant, audience, observer} {
		insertSignedPeerBinding(t, st.db, channel.Channel().ID(), member,
			fmt.Sprintf("artifact-source-active-%d", index), model.BindingPending,
			model.ReachabilityUnknown, channel.Channel().UpdatedAt())
		insertPeerCursorForMeshTest(t, st, channel.Channel().ID(), member)
		mustExec(t, st, `UPDATE peer_bindings SET state='active'
			WHERE channel_id=? AND peer_id=?`, channel.Channel().ID().String(),
			member.Identity().PeerID().String())
	}
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), pending, "artifact-source-pending",
		model.BindingPending, model.ReachabilityUnknown, channel.Channel().UpdatedAt())
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), revoked, "artifact-source-revoked",
		model.BindingRevoked, model.ReachabilityUnknown, channel.Channel().UpdatedAt())

	rootClosure, root, block := newArtifactSourceClosure(t, "authorized", []byte("authorized block"),
		channel.Channel().UpdatedAt().Add(time.Second))
	if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), rootClosure); err != nil {
		t.Fatalf("checkpoint authorized Artifact closure: %v", err)
	}
	otherClosure, otherRoot, otherBlock := newArtifactSourceClosure(t, "other", []byte("other block"),
		channel.Channel().UpdatedAt().Add(2*time.Second))
	if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), otherClosure); err != nil {
		t.Fatalf("checkpoint other Artifact closure: %v", err)
	}
	event := insertArtifactSourceEvent(t, st, channel, participant.Identity().PeerID(),
		audience.Identity().PeerID(), root, root.VerifiedAt.Add(time.Second))
	return &artifactSourceFixture{store: st, channel: channel, participant: participant,
		audience: audience, observer: observer, pending: pending, revoked: revoked,
		root: root, block: block, otherRoot: otherRoot, otherBlock: otherBlock, event: event}
}

func newArtifactSourceClosure(t *testing.T, name string, content []byte,
	createdAt time.Time,
) (VerifiedArtifactClosure, VerifiedArtifactRoot, VerifiedArtifactBlock) {
	t.Helper()
	blockDigest := model.Sum(content)
	manifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryFile, RootPath: name + ".txt",
		Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
			LogicalPath: name + ".txt", Mode: 0o600, SizeBytes: uint64(len(content)),
			Blocks: []artifactdomain.ManifestBlock{{Digest: blockDigest,
				LengthBytes: uint64(len(content)), OffsetBytes: 0}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	root := VerifiedArtifactRoot{RootDigest: manifest.RootDigest(), Manifest: manifest.CanonicalJSON(),
		ManifestDigest: manifest.ManifestDigest(), TotalBytes: manifest.TotalBytes(),
		CreatedAt: createdAt, VerifiedAt: createdAt.Add(time.Second)}
	block := VerifiedArtifactBlock{Digest: blockDigest, SizeBytes: uint64(len(content)), CreatedAt: createdAt}
	row := VerifiedArtifactRootBlock{RootDigest: root.RootDigest, Ordinal: 0,
		LogicalPath: name + ".txt", LengthBytes: uint64(len(content)),
		BlockDigest: blockDigest, Mode: 0o600}
	return VerifiedArtifactClosure{Roots: []VerifiedArtifactRoot{root},
		Blocks: []VerifiedArtifactBlock{block}, RootBlocks: []VerifiedArtifactRootBlock{row}}, root, block
}

func insertArtifactSourceEvent(t *testing.T, st *Store, channel *testkit.SignedChannel,
	participant, audiencePeer model.PeerID, root VerifiedArtifactRoot, at time.Time,
) model.Event {
	t.Helper()
	workID, _ := model.ParseWorkID("work-artifact-source")
	workRef, err := model.NewWorkRef(channel.Owner().PeerID(), workID)
	if err != nil {
		t.Fatal(err)
	}
	owner, ok := channel.Roster().CurrentMember(channel.Owner().PeerID())
	if !ok {
		t.Fatal("source Channel has no current owner")
	}
	scope, err := model.NewEventScope(channel.Channel().ID(), channel.Owner().PeerID(),
		channel.Owner().OriginEpoch(), 1, 1, owner.Head(), channel.Roster().Head(), workRef)
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{audiencePeer})
	payload, _ := model.JSONFrom(struct {
		Content string `json:"content"`
	}{"artifact source authority"})
	artifactRef, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
	eventID, _ := model.ParseEventID("event-artifact-source")
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-artifact-source",
		Type: model.EventReviewOffered, Audience: audience, Summary: "Artifact source authority",
		Payload: payload, Artifacts: []model.ArtifactRef{artifactRef}, CreatedAt: at, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.PublicationSigningMessage(channel.Channel().ID(), body.Digest())
	if err != nil {
		t.Fatal(err)
	}
	signature := ed25519.Sign(ed25519Private(channel.Owner()), message)
	publication, err := model.AttachSignature(body, signature)
	if err != nil {
		t.Fatal(err)
	}
	participants, _ := model.NewParticipantSnapshot(channel.Channel().ID(),
		channel.Roster().Head().Revision(), channel.Owner().PeerID(), participant)
	state, _ := model.JSONFrom(struct {
		Content string `json:"content"`
	}{"artifact source authority"})
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: workRef,
		ChannelID: channel.Channel().ID(), Participants: participants, Version: 1, Iteration: 1,
		DeadlineUnixNano: at.Add(model.DefaultReviewDeadline).UnixNano(), State: model.WorkOffered,
		StateData: state, UpdatedBy: event.ID(), UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertAcceptedEvent(context.Background(), tx, publication); err != nil {
		t.Fatal(err)
	}
	if err := insertOfferedWork(context.Background(), tx, work, event); err != nil {
		t.Fatal(err)
	}
	if _, err := insertEventArtifactPin(context.Background(), tx, root.RootDigest,
		event.ID(), event.AcceptedAt()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return event
}

func assertArtifactSourceUnavailable(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrArtifactSourceUnavailable) ||
		errors.Is(err, ErrArtifactSourceInvariant) {
		t.Fatalf("Artifact source denial error = %v", err)
	}
}
