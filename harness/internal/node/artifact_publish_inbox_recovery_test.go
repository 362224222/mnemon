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
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestArtifactStageCleanupWorkerRecoversAcceptedInboxAfterAuthorityExpires(
	t *testing.T,
) {
	fixture := newArtifactStageCleanupWorkerFixture(t)
	ctx := context.Background()
	recovery := prepareInboxRecoveryStage(t, fixture, "accepted-inbox-recovery")
	if _, err := fixture.store.AcceptPeerInboxArtifactPublish(ctx,
		store.AcceptPeerInboxArtifactPublishSpec{
			Fence: recovery.claim.Fence(), Owner: recovery.owner,
			At: recovery.stageAt.Add(2 * time.Second),
		}); err != nil {
		t.Fatal(err)
	}
	if !recovery.claim.Fence().LeaseUntil().Before(fixture.now) {
		t.Fatalf("Inbox lease %v did not expire before recovery %v",
			recovery.claim.Fence().LeaseUntil(), fixture.now)
	}

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	database := openArtifactStageCleanupDatabase(t, fixture.database)
	if _, err := database.Exec(`UPDATE channels
		SET status='closed',topic_state='left' WHERE channel_id=?`,
		recovery.signed.Channel().ID().String()); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
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
	if err := fixture.cas.VerifyClosure(ctx, recovery.closure); err != nil {
		t.Fatalf("recovered Inbox final closure: %v", err)
	}
	if _, err := restarted.GetVerifiedArtifactRoot(ctx, recovery.root); err != nil {
		t.Fatalf("recovered Inbox root authority: %v", err)
	}
	checkpoint, err := restarted.ReadPeerInboxArtifactPublish(ctx,
		store.ReadPeerInboxArtifactPublishSpec{
			Fence: recovery.claim.Fence(), Owner: recovery.owner, At: fixture.now,
		})
	if err != nil || checkpoint.State() != store.ArtifactStageReady {
		t.Fatalf("recovered Inbox publish = (%#v,%v)", checkpoint, err)
	}
	if replay, err := restarted.MarkPeerInboxArtifactReady(ctx,
		store.MarkPeerInboxArtifactReadySpec{
			Fence: recovery.claim.Fence(), Owner: recovery.owner, At: fixture.now,
		}); err != nil || !replay.Replayed() {
		t.Fatalf("recovered Inbox Ready replay = (%#v,%v)", replay, err)
	}
}

func TestAcceptPeerInboxArtifactPublishSettlesExpiredTerminalAuthority(
	t *testing.T,
) {
	fixture := newArtifactStageCleanupWorkerFixture(t)
	ctx := context.Background()
	recovery := prepareInboxRecoveryStage(t, fixture,
		"terminal-authority-accept")
	expiredAt := recovery.claim.Fence().LeaseUntil().Add(time.Second)
	spec := store.AcceptPeerInboxArtifactPublishSpec{
		Fence: recovery.claim.Fence(), Owner: recovery.owner, At: expiredAt,
	}
	if _, err := fixture.store.AcceptPeerInboxArtifactPublish(
		ctx, spec); !errors.Is(err, store.ErrPeerInboxArtifactStale) {
		t.Fatalf("nonterminal expired Accept error = %v", err)
	}
	assertInboxRecoveryStillWaiting(t, fixture, recovery)
	terminalAt := expiredAt.Add(time.Second)
	closeInboxRecoveryChannel(t, fixture, recovery, terminalAt)
	spec.At = terminalAt.Add(time.Second)
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := fixture.store.AcceptPeerInboxArtifactPublish(
			ctx, spec); !errors.Is(err, store.ErrPeerInboxArtifactStale) {
			t.Fatalf("terminal Accept replay %d error = %v", attempt, err)
		}
	}
	assertTerminalInboxPublishQuarantined(t, fixture, recovery, spec.At)
	assertInboxRecoveryFinalObjectsMissing(t, fixture, recovery.closure)
}

func TestArtifactStageCleanupWorkerSettlesTerminalInboxAfterPrepareRestart(
	t *testing.T,
) {
	fixture := newArtifactStageCleanupWorkerFixture(t)
	ctx := context.Background()
	recovery := prepareInboxRecoveryStage(t, fixture,
		"terminal-authority-restart")
	terminalAt := recovery.claim.Fence().LeaseUntil().Add(time.Second)
	closeInboxRecoveryChannel(t, fixture, recovery, terminalAt)
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

	cleanupAt := terminalAt.Add(2 * time.Hour)
	page, err := restarted.ScanAcceptedArtifactPublishes(ctx,
		store.ScanAcceptedArtifactPublishesSpec{At: cleanupAt, MaxExamined: 8})
	if err != nil || page.Examined() != 0 || len(page.Candidates()) != 0 {
		t.Fatalf("unaccepted restart recovery page = (%#v,%v)", page, err)
	}
	fixture.clock.now = cleanupAt
	worker := fixture.worker(t, artifactStageCleanupOptions{})
	if err := worker.runCycle(ctx); err != nil {
		t.Fatal(err)
	}
	if got := fixture.stageDirectoryCount(t); got != 0 {
		t.Fatalf("physical Inbox stages after cleanup = %d", got)
	}
	assertTerminalInboxStageCleaned(t, fixture, recovery)
	assertInboxRecoveryFinalObjectsMissing(t, fixture, recovery.closure)

	fixture.clock.now = cleanupAt.Add(2 * time.Hour)
	if err := worker.runCycle(ctx); err != nil {
		t.Fatal(err)
	}
	assertTerminalInboxRelationalStagingRemoved(t, fixture, recovery)
}

func closeInboxRecoveryChannel(t *testing.T,
	fixture *artifactStageCleanupWorkerFixture, recovery inboxRecoveryStage,
	at time.Time,
) {
	t.Helper()
	terminal := recovery.signed.AppendTerminal(t,
		recovery.remote.PeerID(), model.MemberLeft)
	merged, err := fixture.store.MergeChannelRoster(
		context.Background(), store.MergeChannelRosterSpec{
			ChannelID:                    recovery.signed.Channel().ID(),
			AuthenticatedTransportPeerID: recovery.remote.PeerID(),
			Records:                      []model.Member{terminal.Member()},
			At:                           at,
		})
	if err != nil || merged.Channel.Status() != model.ChannelClosed {
		t.Fatalf("terminal Channel merge = (%#v,%v)", merged, err)
	}
}

func assertInboxRecoveryStillWaiting(t *testing.T,
	fixture *artifactStageCleanupWorkerFixture, recovery inboxRecoveryStage,
) {
	t.Helper()
	db := openArtifactStageCleanupDatabase(t, fixture.database)
	defer db.Close()
	var status string
	var diagnostic any
	if err := db.QueryRow(`SELECT status,diagnostic FROM peer_inbox
		WHERE inbox_id=?`, recovery.claim.InboxID().String()).
		Scan(&status, &diagnostic); err != nil {
		t.Fatal(err)
	}
	if status != string(model.InboxWaitingArtifact) || diagnostic != nil {
		t.Fatalf("nonterminal expired Inbox changed = (%q,%v)",
			status, diagnostic)
	}
}

func assertTerminalInboxStageCleaned(t *testing.T,
	fixture *artifactStageCleanupWorkerFixture, recovery inboxRecoveryStage,
) {
	t.Helper()
	db := openArtifactStageCleanupDatabase(t, fixture.database)
	defer db.Close()
	var status, diagnostic, stageState string
	var cleaned any
	if err := db.QueryRow(`SELECT inbox.status,inbox.diagnostic,stage.state,
		stage.cleaned_at FROM peer_inbox inbox
		JOIN peer_inbox_artifact_stages stage ON stage.inbox_id=inbox.inbox_id
		WHERE inbox.inbox_id=? AND stage.generation=?`,
		recovery.claim.InboxID().String(), recovery.owner.Generation()).
		Scan(&status, &diagnostic, &stageState, &cleaned); err != nil {
		t.Fatal(err)
	}
	if status != string(model.InboxQuarantined) ||
		diagnostic != "artifact_authority_terminal" ||
		stageState != string(store.ArtifactStagePublishing) || cleaned == nil {
		t.Fatalf("terminal restart settlement = (%q,%q,%q,%v)",
			status, diagnostic, stageState, cleaned)
	}
	var permanent int
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_pins
		WHERE owner_kind='inbox' AND owner_id=? AND expires_at IS NULL`,
		recovery.claim.InboxID().String()).Scan(&permanent); err != nil {
		t.Fatal(err)
	}
	if permanent != 0 {
		t.Fatalf("terminal restart permanent pins = %d", permanent)
	}
}

type inboxRecoveryStage struct {
	signed  *testkit.SignedChannel
	remote  testkit.Identity
	claim   store.PeerInboxArtifactClaim
	owner   artifact.StageOwner
	closure artifact.Closure
	root    model.Digest
	stageAt time.Time
}

func prepareInboxRecoveryStage(t *testing.T,
	fixture *artifactStageCleanupWorkerFixture, seed string,
) inboxRecoveryStage {
	t.Helper()
	ctx := context.Background()
	remote := testkit.NewIdentity(t, seed+"-remote")
	signed := testkit.NewSignedChannelForOwnerAt(t, seed,
		remote, fixture.now.Add(-30*time.Minute))
	signed.AppendActiveIdentity(t, fixture.identity)
	installRecoveryInboundChannel(t, fixture.database, signed,
		signed.OwnerMember())
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "result.txt"),
		[]byte(seed), 0o600); err != nil {
		t.Fatal(err)
	}
	captureAt := fixture.now.Add(-10 * time.Minute)
	preview := captureInboxRecoveryArtifact(t, workspace, captureAt)
	root := preview.Roots()[0].RootDigest
	publication := recoveryInboxPublication(t, signed, remote, fixture.identity,
		root, fixture.now.Add(-9*time.Minute))
	put, err := fixture.store.PutPeerInbox(ctx, store.PutPeerInboxSpec{
		Publication: publication, TransportPeerID: remote.PeerID(),
		ArrivalSource: model.ArrivalPull, ReceivedAt: fixture.now.Add(-8 * time.Minute),
	})
	if err != nil || put.Disposition != store.PeerInboxStored {
		t.Fatalf("PutPeerInbox() = (%#v,%v)", put, err)
	}
	claimAt := fixture.now.Add(-7 * time.Minute)
	claimed, err := fixture.store.ClaimPeerInboxArtifact(ctx,
		store.ClaimPeerInboxArtifactSpec{
			LeaseOwner: seed + "-worker", At: claimAt,
		})
	if err != nil || !claimed.Found() {
		t.Fatalf("ClaimPeerInboxArtifact() = (%#v,%v)", claimed, err)
	}
	claim := claimed.Claim()
	stageAt := claimAt.Add(time.Second)
	begun, err := fixture.store.BeginPeerInboxArtifactStage(ctx,
		store.BeginPeerInboxArtifactStageSpec{Fence: claim.Fence(), At: stageAt})
	if err != nil {
		t.Fatal(err)
	}
	stage, err := fixture.cas.OpenStage(begun.Owner())
	if err != nil {
		t.Fatal(err)
	}
	capturer, err := artifact.NewCapturer(workspace, func() time.Time {
		return captureAt
	})
	if err != nil {
		t.Fatal(err)
	}
	closure, err := capturer.Capture(ctx, []string{"result.txt"}, stage)
	if err != nil || closure.Roots()[0].RootDigest != root {
		t.Fatalf("staged Inbox closure = (%#v,%v)", closure, err)
	}
	if _, err := fixture.store.PreparePeerInboxArtifactPublish(ctx,
		store.PreparePeerInboxArtifactPublishSpec{
			Fence: claim.Fence(), Owner: begun.Owner(),
			Closure: recoveryStoreClosure(closure), At: stageAt.Add(time.Second),
		}); err != nil {
		t.Fatal(err)
	}
	return inboxRecoveryStage{signed: signed, remote: remote, claim: claim,
		owner: begun.Owner(), closure: closure, root: root, stageAt: stageAt}
}

func assertTerminalInboxPublishQuarantined(t *testing.T,
	fixture *artifactStageCleanupWorkerFixture, recovery inboxRecoveryStage,
	acceptAt time.Time,
) {
	t.Helper()
	db := openArtifactStageCleanupDatabase(t, fixture.database)
	defer db.Close()
	var status, diagnostic string
	var leaseOwner, leaseUntil any
	if err := db.QueryRow(`SELECT status,diagnostic,lease_owner,lease_until
		FROM peer_inbox WHERE inbox_id=?`, recovery.claim.InboxID().String()).
		Scan(&status, &diagnostic, &leaseOwner, &leaseUntil); err != nil {
		t.Fatal(err)
	}
	if status != string(model.InboxQuarantined) ||
		diagnostic != "artifact_authority_terminal" ||
		leaseOwner != nil || leaseUntil != nil {
		t.Fatalf("terminal Inbox projection = (%q,%q,%v,%v)",
			status, diagnostic, leaseOwner, leaseUntil)
	}
	var pins, permanent int
	var expiry string
	if err := db.QueryRow(`SELECT COUNT(*),COALESCE(SUM(expires_at IS NULL),0),
		COALESCE(MIN(expires_at),'') FROM artifact_pins
		WHERE owner_kind='inbox' AND owner_id=?`,
		recovery.claim.InboxID().String()).Scan(&pins, &permanent, &expiry); err != nil {
		t.Fatal(err)
	}
	if pins != len(recovery.claim.RequiredArtifactRoots()) || permanent != 0 ||
		expiry < cleanupStoreTime(acceptAt.Add(time.Hour)) {
		t.Fatalf("terminal Inbox pins = (%d permanent=%d expiry=%q)",
			pins, permanent, expiry)
	}
	var stageState, rootState string
	var cleaned any
	if err := db.QueryRow(`SELECT state,cleaned_at FROM peer_inbox_artifact_stages
		WHERE inbox_id=? AND generation=?`, recovery.claim.InboxID().String(),
		recovery.owner.Generation()).Scan(&stageState, &cleaned); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT state FROM artifact_roots WHERE root_digest=?`,
		recovery.root.String()).Scan(&rootState); err != nil {
		t.Fatal(err)
	}
	if stageState != string(store.ArtifactStagePublishing) ||
		cleaned != nil || rootState != "staged" {
		t.Fatalf("terminal staging state = (%q,%v,%q)",
			stageState, cleaned, rootState)
	}
}

func assertInboxRecoveryFinalObjectsMissing(t *testing.T,
	fixture *artifactStageCleanupWorkerFixture, closure artifact.Closure,
) {
	t.Helper()
	for _, root := range closure.Roots() {
		assertPathMissing(t, fixture.finalPath(root.ManifestDigest))
	}
	for _, block := range closure.Blocks() {
		assertPathMissing(t, fixture.finalPath(block.Digest))
	}
}

func assertTerminalInboxRelationalStagingRemoved(t *testing.T,
	fixture *artifactStageCleanupWorkerFixture, recovery inboxRecoveryStage,
) {
	t.Helper()
	db := openArtifactStageCleanupDatabase(t, fixture.database)
	defer db.Close()
	queries := map[string]string{
		"live stage": `SELECT COUNT(*) FROM peer_inbox_artifact_stages
			WHERE inbox_id=? AND cleaned_at IS NULL`,
		"Ready stage": `SELECT COUNT(*) FROM peer_inbox_artifact_stages
			WHERE inbox_id=? AND state='ready'`,
		"pin": `SELECT COUNT(*) FROM artifact_pins
			WHERE owner_kind='inbox' AND owner_id=?`,
		"projection": `SELECT COUNT(*) FROM peer_inbox_artifact_roots
			WHERE inbox_id=?`,
		"root": `SELECT COUNT(*) FROM artifact_roots WHERE root_digest=?`,
	}
	for name, query := range queries {
		argument := recovery.claim.InboxID().String()
		if name == "root" {
			argument = recovery.root.String()
		}
		var count int
		if err := db.QueryRow(query, argument).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s after terminal cleanup = (%d,%v)", name, count, err)
		}
	}
	var permanent, renewReceipts int
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_pins
		WHERE owner_kind='inbox' AND owner_id=? AND expires_at IS NULL`,
		recovery.claim.InboxID().String()).Scan(&permanent); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM peer_inbox_artifact_renew_receipts
		WHERE inbox_id=?`, recovery.claim.InboxID().String()).Scan(&renewReceipts); err != nil {
		t.Fatal(err)
	}
	if permanent != 0 || renewReceipts != 0 {
		t.Fatalf("terminal cleanup ownership = (permanent=%d renew=%d)",
			permanent, renewReceipts)
	}
}

func captureInboxRecoveryArtifact(t *testing.T, workspace string,
	at time.Time,
) artifact.Closure {
	t.Helper()
	cas, err := artifact.NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	operationID, _ := model.ParseOperationID("preview-accepted-inbox-recovery")
	owner, err := artifact.NewOperationStageOwner(operationID, 1)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := cas.OpenStage(owner)
	if err != nil {
		t.Fatal(err)
	}
	capturer, err := artifact.NewCapturer(workspace, func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	closure, err := capturer.Capture(context.Background(), []string{"result.txt"}, stage)
	if err != nil {
		t.Fatal(err)
	}
	return closure
}

func recoveryInboxPublication(t *testing.T, signed *testkit.SignedChannel,
	remote, local testkit.Identity, root model.Digest, at time.Time,
) model.SignedPublication {
	t.Helper()
	workID, _ := model.ParseWorkID("work-accepted-inbox-recovery")
	work, err := model.NewWorkRef(local.PeerID(), workID)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := model.NewEventScope(signed.Channel().ID(), remote.PeerID(),
		remote.OriginEpoch(), 1, 1, signed.OwnerMember().Member().Head(),
		signed.Roster().Head(), work)
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{local.PeerID()})
	ref, _ := model.NewArtifactRef(root, model.ArtifactProduced)
	eventID, _ := model.ParseEventID("event-accepted-inbox-recovery")
	payload, _ := model.NewJSON([]byte(
		`{"content":"accepted Inbox recovery","iteration":1,"work_version":1}`))
	event, err := model.NewEvent(model.EventSpec{
		ID: eventID, Scope: scope, Source: model.EventSourceLocal,
		ActorPrincipal: "principal-accepted-inbox-recovery",
		Type:           model.EventReviewAcceptRequested, Audience: audience,
		Summary: "accepted Inbox recovery", Payload: payload,
		Artifacts: []model.ArtifactRef{ref}, CreatedAt: at, AcceptedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.PublicationSigningMessage(scope.ChannelID(), body.Digest())
	if err != nil {
		t.Fatal(err)
	}
	private, err := remote.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := private.Raw()
	if err != nil {
		t.Fatal(err)
	}
	publication, err := model.AttachSignature(body,
		ed25519.Sign(ed25519.PrivateKey(raw), message))
	if err != nil {
		t.Fatal(err)
	}
	return publication
}
