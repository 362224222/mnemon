package node

import (
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestArtifactStageCleanupWorkerRecoversAcceptedOperationPublishAfterRestart(
	t *testing.T,
) {
	fixture := newArtifactStageCleanupWorkerFixture(t)
	ctx := context.Background()
	prepared := prepareAcceptedRecoveryOperation(t, fixture, "operation",
		"accepted operation recovery")
	operation, closure, durable := prepared.operation, prepared.closure, prepared.durable

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.OpenExisting(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	fixture.store = restarted
	worker := fixture.worker(t, artifactStageCleanupOptions{})
	if err := worker.runCycle(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.cas.VerifyClosure(ctx, closure); err != nil {
		t.Fatalf("recovered final closure: %v", err)
	}
	if _, err := restarted.GetVerifiedArtifactRoot(
		ctx, durable.Roots[0].RootDigest); err != nil {
		t.Fatalf("recovered root authority: %v", err)
	}
	checkpoint, found, err := restarted.ReadCommittedOperationArtifactPublish(ctx,
		store.ReadCommittedOperationArtifactPublishSpec{
			OperationID: operation.ID(), At: fixture.now,
		})
	if err != nil || !found || checkpoint.State() != store.ArtifactStageReady {
		t.Fatalf("recovered operation publish = (%#v,%t,%v)",
			checkpoint, found, err)
	}
	fixture.clock.now = fixture.now.Add(2 * time.Hour)
	if err := worker.runCycle(ctx); err != nil {
		t.Fatal(err)
	}
	fixture.assertCleaned(t, operation.ID().String(), true)
	cleaned, found, err := restarted.ReadCommittedOperationArtifactPublish(ctx,
		store.ReadCommittedOperationArtifactPublishSpec{
			OperationID: operation.ID(), At: fixture.clock.now,
		})
	if err != nil || !found || cleaned.State() != store.ArtifactStageReady {
		t.Fatalf("cleaned ready operation publish = (%#v,%t,%v)",
			cleaned, found, err)
	}
	if replay, err := restarted.MarkOperationArtifactReady(ctx,
		store.MarkOperationArtifactReadySpec{
			Fence: cleaned.Fence(), At: fixture.clock.now,
		}); err != nil || !replay.Replayed() {
		t.Fatalf("cleaned ready replay = (%#v,%v)", replay, err)
	}
}

func TestArtifactStageCleanupWorkerContinuesAcceptedPageAfterCorruptPublish(
	t *testing.T,
) {
	fixture := newArtifactStageCleanupWorkerFixture(t)
	first := prepareAcceptedRecoveryOperation(t, fixture, "a-corrupt",
		"corrupt accepted operation recovery")
	corruptOnlyRecoveryStage(t, fixture.cas)
	fixture.now = fixture.now.Add(time.Second)
	fixture.clock.now = fixture.now
	second := prepareAcceptedRecoveryOperation(t, fixture, "b-valid",
		"valid accepted operation recovery")

	page, err := fixture.store.ScanAcceptedArtifactPublishes(context.Background(),
		store.ScanAcceptedArtifactPublishesSpec{
			At: fixture.now, MaxExamined: 2,
		})
	if err != nil || len(page.Candidates()) != 2 ||
		page.Candidates()[0].OperationID() != first.operation.ID() ||
		page.Candidates()[1].OperationID() != second.operation.ID() {
		t.Fatalf("accepted recovery page = (%#v,%v)", page, err)
	}
	worker := fixture.worker(t, artifactStageCleanupOptions{MaxExamined: 2})
	err = worker.runCycle(context.Background())
	if !errors.Is(err, artifact.ErrCASCorruption) ||
		!errors.Is(err, ErrArtifactStageCleanup) {
		t.Fatalf("corrupt accepted recovery error = %v", err)
	}
	firstCheckpoint, found, firstErr :=
		fixture.store.ReadCommittedOperationArtifactPublish(context.Background(),
			store.ReadCommittedOperationArtifactPublishSpec{
				OperationID: first.operation.ID(), At: fixture.now,
			})
	if firstErr != nil || !found ||
		firstCheckpoint.State() != store.ArtifactStagePublishing {
		t.Fatalf("corrupt accepted publish = (%#v,%t,%v)",
			firstCheckpoint, found, firstErr)
	}
	secondCheckpoint, found, secondErr :=
		fixture.store.ReadCommittedOperationArtifactPublish(context.Background(),
			store.ReadCommittedOperationArtifactPublishSpec{
				OperationID: second.operation.ID(), At: fixture.now,
			})
	if secondErr != nil || !found ||
		secondCheckpoint.State() != store.ArtifactStageReady {
		t.Fatalf("later accepted publish = (%#v,%t,%v)",
			secondCheckpoint, found, secondErr)
	}
	if err := fixture.cas.VerifyClosure(context.Background(), second.closure); err != nil {
		t.Fatalf("later accepted final closure: %v", err)
	}
}

func corruptOnlyRecoveryStage(t *testing.T, cas *artifact.CAS) {
	t.Helper()
	directories, err := os.ReadDir(filepath.Join(cas.Root(), ".staging"))
	if err != nil || len(directories) != 1 {
		t.Fatalf("recovery stage directories = (%d,%v)", len(directories), err)
	}
	entries, err := os.ReadDir(filepath.Join(cas.Root(), ".staging",
		directories[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == ".owner.json" {
			continue
		}
		path := filepath.Join(cas.Root(), ".staging", directories[0].Name(),
			entry.Name())
		if err := os.WriteFile(path, []byte("corrupt accepted stage"), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("accepted stage has no object to corrupt")
}

type acceptedRecoveryOperation struct {
	operation model.Operation
	fence     store.OperationArtifactStageFence
	closure   artifact.Closure
	durable   store.VerifiedArtifactClosure
}

func prepareAcceptedRecoveryOperation(t *testing.T,
	fixture *artifactStageCleanupWorkerFixture, suffix, content string,
) acceptedRecoveryOperation {
	t.Helper()
	ctx := context.Background()
	operationID, _ := model.ParseOperationID("cleanup-recover-" + suffix)
	runID, _ := model.ParseRunID("run-artifact-stage-cleanup")
	createdAt := fixture.now.Add(-10 * time.Minute)
	leaseUntil := fixture.now.Add(10 * time.Minute)
	operation, err := model.NewOperation(model.OperationSpec{
		ID: operationID, ProfileID: model.TeamworkProfileID(), AgentRunID: runID,
		ClientKeyHash: model.Sum([]byte("cleanup-recover-" + suffix + "-key")),
		Kind:          model.OperationTeamworkOffer,
		RequestDigest: model.Sum([]byte("cleanup-recover-" + suffix + "-request")),
		Status:        model.OperationStarted,
		LeaseOwner:    "cleanup-recover-" + suffix + "-owner",
		LeaseUntil:    &leaseUntil,
		CreatedAt:     createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.ReserveOperation(ctx, operation, createdAt); err != nil {
		t.Fatal(err)
	}
	publishAt := fixture.now.Add(-time.Minute)
	begun, err := fixture.store.BeginOperationArtifactStage(ctx,
		store.BeginOperationArtifactStageSpec{
			OperationID: operation.ID(), LeaseOwner: operation.LeaseOwner(),
			LeaseUntil: leaseUntil, At: publishAt,
		})
	if err != nil {
		t.Fatal(err)
	}
	stage, err := fixture.cas.OpenStage(begun.Fence().Owner())
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "result.txt"),
		[]byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	capturer, err := artifact.NewCapturer(workspace, func() time.Time {
		return publishAt
	})
	if err != nil {
		t.Fatal(err)
	}
	closure, err := capturer.Capture(ctx, []string{"result.txt"}, stage)
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.VerifyClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}
	durable := recoveryStoreClosure(closure)
	if _, err := fixture.store.PrepareOperationArtifactPublish(ctx,
		store.PrepareOperationArtifactPublishSpec{
			Fence: begun.Fence(), Capture: closure.Checkpoint(),
			Closure: durable, At: publishAt,
		}); err != nil {
		t.Fatal(err)
	}
	commitRecoveryOperationAcceptance(t, fixture, operation,
		durable.Roots[0], fixture.now, suffix)
	return acceptedRecoveryOperation{
		operation: operation, fence: begun.Fence(), closure: closure, durable: durable,
	}
}

type recoveryEventClock struct{ at time.Time }

func (clock recoveryEventClock) Now() time.Time { return clock.at }

func commitRecoveryOperationAcceptance(t *testing.T,
	fixture *artifactStageCleanupWorkerFixture, operation model.Operation,
	root store.VerifiedArtifactRoot, at time.Time, suffix string,
) {
	t.Helper()
	signed := testkit.NewSignedChannelForOwnerAt(t, "cleanup-recover-"+suffix,
		fixture.identity, at.Add(-30*time.Minute))
	reviewer := signed.AppendActive(t, "cleanup-recover-"+suffix+"-reviewer")
	installRecoveryAcceptanceChannel(t, fixture.database, signed, reviewer)
	audience, err := model.NewAudience([]model.PeerID{reviewer.Identity().PeerID()})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := fixture.store.PrepareLocalAdmission(context.Background(),
		signed.Channel().ID(), audience, 1)
	if err != nil {
		t.Fatal(err)
	}
	signer := recoveryPublicationSigner(t, fixture.identity)
	factory, err := eventpkg.NewFactory(recoveryEventClock{at: at.Add(-time.Second)}, signer)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := eventpkg.NewOfferCandidate("recover accepted Artifact publish "+suffix,
		time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	workID, _ := model.ParseWorkID("work-cleanup-recover-" + suffix)
	workRef, err := model.NewWorkRef(fixture.identity.PeerID(), workID)
	if err != nil {
		t.Fatal(err)
	}
	eventID, _ := model.ParseEventID("event-cleanup-recover-" + suffix)
	eventScope, err := scope.EventScope(0, workRef)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := eventpkg.NewAdmissionStamp(eventpkg.AdmissionStampSpec{
		Node: scope.Node(), Profile: scope.Profile(), EventID: eventID,
		ChannelID: signed.Channel().ID(), WorkRef: workRef,
		OriginSequence: eventScope.OriginSequence(), ChannelSequence: eventScope.ChannelSequence(),
		OriginMember: eventScope.OriginMember(), PublicationRoster: eventScope.PublicationRoster(),
		Audience: audience, WorkVersion: 1, Iteration: 1,
		Artifacts: []model.ArtifactRef{ref},
	})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := factory.AdmitAgent(context.Background(), stamp, candidate)
	if err != nil {
		t.Fatal(err)
	}
	participants, err := model.NewParticipantSnapshot(signed.Channel().ID(),
		eventScope.PublicationRoster().Revision(), fixture.identity.PeerID(),
		reviewer.Identity().PeerID())
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewReviewWork(model.ReviewWorkSpec{
		Ref: workRef, ChannelID: signed.Channel().ID(), Participants: participants,
		Version: 1, Iteration: 1, DeadlineUnixNano: bundle.WorkDeadlineUnixNano(),
		State: model.WorkOffered, StateData: bundle.Event().Payload(),
		UpdatedBy: bundle.Event().ID(), UpdatedAt: at.Add(-time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	creation, err := store.NewWorkCreation(work)
	if err != nil {
		t.Fatal(err)
	}
	authority := &store.LocalOperationAuthority{
		ID: operation.ID(), Kind: operation.Kind(),
		RequestDigest: operation.RequestDigest(), LeaseOwner: operation.LeaseOwner(),
	}
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(),
		store.LocalAcceptanceSpec{Scope: scope,
			Items: []store.LocalAcceptanceItem{{
				Publication: bundle.Publication(), Work: &creation,
			}},
			Operation: authority,
		}, at); err != nil {
		t.Fatal(err)
	}
}

func installRecoveryAcceptanceChannel(t *testing.T, database string,
	signed *testkit.SignedChannel, reviewer testkit.MemberFixture,
) {
	t.Helper()
	installRecoveryChannel(t, database, signed, reviewer, true)
}

func installRecoveryInboundChannel(t *testing.T, database string,
	signed *testkit.SignedChannel, remote testkit.MemberFixture,
) {
	t.Helper()
	installRecoveryChannel(t, database, signed, remote, false)
}

func installRecoveryChannel(t *testing.T, database string,
	signed *testkit.SignedChannel, bound testkit.MemberFixture,
	localPublication bool,
) {
	t.Helper()
	db := openArtifactStageCleanupDatabase(t, database)
	defer db.Close()
	projection := signed.Projection()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,
		owner_public_key,descriptor_json,descriptor_digest,descriptor_signature,
		member_limit,roster_head_revision,roster_head_hash,status,topic_state,
		created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		projection.ChannelID, projection.Name, projection.LocalAlias,
		projection.OwnerPeerID, projection.OwnerPublicKey, projection.DescriptorJSON,
		projection.DescriptorDigest, projection.DescriptorSignature,
		projection.MemberLimit, projection.RosterHeadRevision, projection.RosterHeadHash,
		projection.Status, string(model.TopicJoined),
		cleanupStoreTime(signed.Channel().CreatedAt()),
		cleanupStoreTime(signed.Channel().UpdatedAt())); err != nil {
		t.Fatal(err)
	}
	members := signed.Members()
	for index, member := range signed.MemberProjections() {
		if _, err := tx.Exec(`INSERT INTO channel_members(channel_id,revision,record_hash,
			previous_hash,member_peer_id,origin_epoch,display_label,public_key,
			multiaddrs_json,protocols_json,limits_json,status,signed_record_json,
			owner_signature,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			member.ChannelID, member.Revision, member.RecordHash,
			recoveryNullableBytes(member.PreviousHash), member.MemberPeerID,
			member.OriginEpoch, member.DisplayLabel, member.PublicKey,
			member.MultiaddrsJSON, member.ProtocolsJSON, member.LimitsJSON,
			member.Status, member.SignedRecordJSON, member.OwnerSignature,
			cleanupStoreTime(members[index].Member().CreatedAt())); err != nil {
			t.Fatal(err)
		}
	}
	member := bound.Projection()
	joinedAt := signed.Channel().UpdatedAt()
	if _, err := tx.Exec(`INSERT INTO peer_bindings(channel_id,peer_id,origin_epoch,
		effective_alias,public_key,multiaddrs_json,protocols_json,limits_json,
		member_revision,member_record_hash,state,reachability,joined_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, signed.Channel().ID().String(),
		member.MemberPeerID, member.OriginEpoch, "recovery-reviewer", member.PublicKey,
		member.MultiaddrsJSON, member.ProtocolsJSON, member.LimitsJSON,
		member.Revision, member.RecordHash, string(model.BindingPending),
		string(model.ReachabilityUnknown), cleanupStoreTime(joinedAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO peer_cursors(channel_id,origin_peer_id,
		origin_epoch,baseline_channel_seq,contiguous_channel_seq,
		observed_channel_seq,updated_at) VALUES(?,?,?,0,0,0,?)`,
		signed.Channel().ID().String(), member.MemberPeerID, member.OriginEpoch,
		cleanupStoreTime(joinedAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE peer_bindings SET state='active'
		WHERE channel_id=? AND peer_id=?`, signed.Channel().ID().String(),
		member.MemberPeerID); err != nil {
		t.Fatal(err)
	}
	if !localPublication {
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := tx.Exec(`INSERT INTO publication_epochs(channel_id,origin_peer_id,
		origin_epoch,source_floor_channel_seq,source_head_channel_seq,updated_at)
		VALUES(?,?,?,1,0,?)`, signed.Channel().ID().String(),
		signed.Owner().PeerID().String(), signed.Owner().OriginEpoch().String(),
		cleanupStoreTime(joinedAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO peer_pull_acks(channel_id,target_peer_id,
		origin_peer_id,origin_epoch,baseline_channel_seq,acknowledged_channel_seq,
		baseline_confirmed_at,updated_at) VALUES(?,?,?,?,0,0,NULL,?)`,
		signed.Channel().ID().String(), member.MemberPeerID,
		signed.Owner().PeerID().String(), signed.Owner().OriginEpoch().String(),
		cleanupStoreTime(joinedAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE peer_pull_acks SET baseline_confirmed_at=?
		WHERE channel_id=? AND target_peer_id=?`, cleanupStoreTime(joinedAt),
		signed.Channel().ID().String(), member.MemberPeerID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func recoveryPublicationSigner(t *testing.T,
	identity testkit.Identity,
) eventpkg.PublicationSigner {
	t.Helper()
	private, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := private.Raw()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := eventpkg.NewEd25519Signer(ed25519.PrivateKey(raw))
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

func recoveryNullableBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func recoveryStoreClosure(closure artifact.Closure) store.VerifiedArtifactClosure {
	roots := closure.Roots()
	blocks := closure.Blocks()
	mappings := closure.BlockMap()
	result := store.VerifiedArtifactClosure{
		Roots:      make([]store.VerifiedArtifactRoot, len(roots)),
		Blocks:     make([]store.VerifiedArtifactBlock, len(blocks)),
		RootBlocks: make([]store.VerifiedArtifactRootBlock, len(mappings)),
	}
	for index, root := range roots {
		result.Roots[index] = store.VerifiedArtifactRoot{
			RootDigest: root.RootDigest, Manifest: root.Manifest,
			ManifestDigest: root.ManifestDigest, TotalBytes: root.TotalBytes,
			CreatedAt: root.CreatedAt, VerifiedAt: root.VerifiedAt,
		}
	}
	for index, block := range blocks {
		result.Blocks[index] = store.VerifiedArtifactBlock{
			Digest: block.Digest, SizeBytes: block.SizeBytes,
			CreatedAt: block.CreatedAt,
		}
	}
	for index, row := range mappings {
		result.RootBlocks[index] = store.VerifiedArtifactRootBlock{
			RootDigest: row.RootDigest, Ordinal: row.Ordinal,
			LogicalPath: row.LogicalPath, OffsetBytes: row.OffsetBytes,
			LengthBytes: row.LengthBytes, BlockDigest: row.BlockDigest,
			Mode: row.Mode,
		}
	}
	return result
}
