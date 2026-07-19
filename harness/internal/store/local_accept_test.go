package store

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestCommitLocalAcceptancePersistsCompleteEvidenceAndReplaysAfterRestart(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	operation, authority := fixture.reserveOffer(t, "single", nil)
	root := verifiedRoot(t, "accept-root", `{"entries":[],"kind":"review","total_bytes":0}`, 0)
	if _, err := fixture.store.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	checkpointOperationRoot(t, fixture, operation, authority.LeaseOwner, root)
	artifact, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
	spec := fixture.offer(t, authority, "single", fixture.reviewers,
		[]model.ArtifactRef{artifact}, nil)
	result, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, fixture.now.Add(time.Second))
	if err != nil || result.Replayed || !strings.Contains(result.Receipt.String(), root.RootDigest.String()) {
		t.Fatalf("CommitLocalAcceptance() = (%#v, %v)", result, err)
	}
	assertAcceptanceCounts(t, fixture.store, []int{1, 1, 2, 0, 1, 1, 1, 3})
	assertAcceptanceHeads(t, fixture.store, 2, 1)

	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	replay, err := restarted.CommitLocalAcceptance(context.Background(), LocalAcceptanceSpec{Operation: authority}, time.Time{})
	if err != nil || !replay.Replayed || replay.Receipt.String() != result.Receipt.String() {
		t.Fatalf("restart replay = (%#v, %v)", replay, err)
	}
	assertAcceptanceCounts(t, restarted, []int{1, 1, 2, 0, 1, 1, 1, 3})

	fixture.now = fixture.now.Add(2 * time.Second)
	_, secondAuthority := fixture.reserveOffer(t, "reference", nil)
	referenced, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactReferenced)
	referenceSpec := fixture.offer(t, secondAuthority, "reference", fixture.reviewers,
		[]model.ArtifactRef{referenced}, nil)
	referenceSpec.AuthorizedReferences = []model.Digest{root.RootDigest}
	if _, err := restarted.CommitLocalAcceptance(context.Background(), referenceSpec, fixture.now.Add(time.Second)); err != nil {
		t.Fatalf("referenced acceptance error = %v", err)
	}
	assertAcceptanceCounts(t, restarted, []int{2, 2, 4, 0, 2, 2, 1, 6})
}

func TestApplyLocalAcceptanceTxParticipatesInCallerTransaction(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	operation, authority := fixture.reserveOffer(t, "outer-transaction", nil)
	spec := fixture.offer(t, authority, "outer-transaction", fixture.reviewers, nil, nil)
	committedAt := fixture.now.Add(time.Second)

	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	receipt, err := applyLocalAcceptanceTx(context.Background(), tx, spec, committedAt, false)
	if err != nil || receipt.IsZero() {
		t.Fatalf("applyLocalAcceptanceTx() = (%s, %v)", receipt, err)
	}
	var events, works, publications, deliveries int
	if err := tx.QueryRow(`SELECT (SELECT COUNT(*) FROM events),(SELECT COUNT(*) FROM works),
		(SELECT COUNT(*) FROM gossip_publications),(SELECT COUNT(*) FROM peer_deliveries)`).
		Scan(&events, &works, &publications, &deliveries); err != nil {
		t.Fatal(err)
	}
	if events != 1 || works != 1 || publications != 1 || deliveries != 1 {
		t.Fatalf("transaction-local evidence = (%d,%d,%d,%d), want all one",
			events, works, publications, deliveries)
	}
	inside, err := readOperationByID(context.Background(), tx, operation.ID())
	if err != nil || inside.Status() != model.OperationCommitted {
		t.Fatalf("transaction-local operation = (%#v, %v)", inside, err)
	}
	var nextOrigin, publicationHead uint64
	if err := tx.QueryRow(`SELECT n.next_origin_seq,p.source_head_channel_seq FROM node n
		JOIN publication_epochs p ON p.origin_peer_id=n.peer_id AND p.origin_epoch=n.origin_epoch`).
		Scan(&nextOrigin, &publicationHead); err != nil {
		t.Fatal(err)
	}
	if nextOrigin != 2 || publicationHead != 1 {
		t.Fatalf("transaction-local heads = (%d,%d), want (2,1)", nextOrigin, publicationHead)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
	assertAcceptanceHeads(t, fixture.store, 1, 0)
	assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)

	committed, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, committedAt)
	if err != nil || committed.Replayed || committed.Receipt.String() != receipt.String() {
		t.Fatalf("public acceptance after outer rollback = (%#v, %v), tx receipt %s",
			committed, err, receipt)
	}
	terminalTx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyLocalAcceptanceTx(context.Background(), terminalTx, spec, committedAt, false); !errors.Is(err, ErrOperationTerminal) {
		_ = terminalTx.Rollback()
		t.Fatalf("direct transaction core terminal error = %v", err)
	}
	if err := terminalTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	replay, err := fixture.store.CommitLocalAcceptance(context.Background(),
		LocalAcceptanceSpec{Operation: authority}, time.Time{})
	if err != nil || !replay.Replayed || replay.Receipt.String() != committed.Receipt.String() {
		t.Fatalf("public acceptance replay = (%#v, %v)", replay, err)
	}
}

func TestCommitLocalAcceptanceTeamBatchIsAllOrNothing(t *testing.T) {
	t.Run("committed", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 2)
		_, authority := fixture.reserveOffer(t, "team", nil)
		spec := fixture.offer(t, authority, "team", fixture.reviewers, nil, nil)
		if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, fixture.now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		assertAcceptanceCounts(t, fixture.store, []int{2, 2, 4, 0, 2, 2, 0, 0})
		assertAcceptanceHeads(t, fixture.store, 3, 2)
	})
	t.Run("rolled back", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 2)
		operation, authority := fixture.reserveOffer(t, "team-rollback", nil)
		spec := fixture.offer(t, authority, "team-rollback", fixture.reviewers, nil, nil)
		workSpec := spec.Items[1].Work.Work.Spec()
		workSpec.StateData, _ = model.NewJSON([]byte(`{"tampered":true}`))
		tampered, _ := model.NewReviewWork(workSpec)
		spec.Items[1].Work.Work = tampered
		if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, fixture.now.Add(time.Second)); err == nil {
			t.Fatal("tampered team batch was accepted")
		}
		assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
		var status string
		if err := fixture.store.db.QueryRow("SELECT status FROM operations WHERE operation_id=?", operation.ID().String()).Scan(&status); err != nil || status != "started" {
			t.Fatalf("operation after rollback = (%q, %v)", status, err)
		}
	})
	t.Run("asset result bound", func(t *testing.T) {
		fixture := newAcceptanceFixtureWithPolicy(t, 2, acceptanceActionPolicy(t, 1))
		operation, authority := fixture.reserveOffer(t, "team-asset-bound", nil)
		spec := fixture.offer(t, authority, "team-asset-bound", fixture.reviewers, nil, nil)
		if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
			fixture.now.Add(time.Second)); !errors.Is(err, ErrOperationMismatch) {
			t.Fatalf("asset-bounded acceptance error = %v", err)
		}
		assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
		assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)
	})
}

type acceptanceFixture struct {
	store      *Store
	path       string
	node       model.Node
	profile    model.Profile
	policy     model.TeamworkActionPolicy
	channel    model.ChannelID
	reviewers  []model.PeerID
	privateKey ed25519.PrivateKey
	now        time.Time
}

func newAcceptanceFixture(t *testing.T, reviewerCount int) *acceptanceFixture {
	t.Helper()
	return newAcceptanceFixtureWithPolicy(t, reviewerCount,
		acceptanceActionPolicy(t, model.MaxChildWorks))
}

func newAcceptanceFixtureWithPolicy(t *testing.T, reviewerCount int,
	policy model.TeamworkActionPolicy,
) *acceptanceFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &acceptanceFixture{store: st, path: path, policy: policy,
		now: time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)}
	t.Cleanup(func() {
		if fixture.store != nil {
			_ = fixture.store.Close()
		}
	})
	base := fixture.now.Add(-time.Hour)
	signed := testkit.NewSignedChannelAt(t, "accept-"+t.Name(), base.Add(time.Minute))
	node, profile := signedBootstrapValues(t, signed.Owner(), "principal-accept", "/workspace/accept", base)
	nodeSpec, profileSpec := node.Spec(), profile.Spec()
	nodeSpec.ActiveAssetRevision = policy.AssetRevision().String()
	profileSpec.ActiveAssetRevision = policy.AssetRevision().String()
	node, err = model.NewNode(nodeSpec)
	if err != nil {
		t.Fatal(err)
	}
	profile, err = model.NewProfile(profileSpec)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node, fixture.profile = activateTestNode(t, st, node, profile)
	fixture.channel = signed.Channel().ID()
	fixture.privateKey = ed25519Private(signed.Owner())
	identities := make([]testkit.Identity, reviewerCount)
	for index := 0; index < reviewerCount; index++ {
		identities[index] = testkit.NewIdentity(t, fmt.Sprintf("peer-accept-reviewer-%c", 'a'+index))
	}
	sort.Slice(identities, func(left, right int) bool {
		comparison, err := model.ComparePeerIDs(identities[left].PeerID(), identities[right].PeerID())
		if err != nil {
			t.Fatal(err)
		}
		return comparison < 0
	})
	for _, identity := range identities {
		signed.AppendActiveIdentity(t, identity)
		fixture.reviewers = append(fixture.reviewers, identity.PeerID())
	}
	installAcceptanceChannel(t, fixture, signed)
	return fixture
}

func acceptanceActionPolicy(t testing.TB, offerMax uint8) model.TeamworkActionPolicy {
	t.Helper()
	return acceptanceActionPolicyForOperations(t, offerMax, acceptanceActionOperations(), model.Digest{})
}

func acceptanceActionOperations() []model.OperationKind {
	return []model.OperationKind{model.OperationTeamworkOffer, model.OperationTeamworkAccept,
		model.OperationTeamworkDecline, model.OperationTeamworkDeliver, model.OperationTeamworkRework,
		model.OperationTeamworkClose, model.OperationTeamworkCancel}
}

func acceptanceActionPolicyForOperations(t testing.TB, offerMax uint8,
	operations []model.OperationKind, revision model.Digest,
) model.TeamworkActionPolicy {
	t.Helper()
	entries := make([]model.TeamworkActionPolicyEntrySpec, len(operations))
	for index, operation := range operations {
		maxResults := uint8(1)
		var contexts []model.TeamworkActionContext
		switch operation {
		case model.OperationTeamworkOffer:
			contexts = []model.TeamworkActionContext{model.TeamworkActionContextNone,
				model.TeamworkActionContextReviewerActive, model.TeamworkActionContextReviewerRework}
		case model.OperationTeamworkAccept, model.OperationTeamworkDecline:
			contexts = []model.TeamworkActionContext{model.TeamworkActionContextReviewerOffered}
		case model.OperationTeamworkDeliver:
			contexts = []model.TeamworkActionContext{model.TeamworkActionContextReviewerActive,
				model.TeamworkActionContextReviewerRework, model.TeamworkActionContextParentResume}
		case model.OperationTeamworkRework:
			contexts = []model.TeamworkActionContext{model.TeamworkActionContextHomeDeliveredIteration1}
		case model.OperationTeamworkClose:
			contexts = []model.TeamworkActionContext{model.TeamworkActionContextHomeDelivered}
		case model.OperationTeamworkCancel:
			contexts = []model.TeamworkActionContext{model.TeamworkActionContextHomeNonterminal}
		}
		if operation == model.OperationTeamworkOffer {
			maxResults = offerMax
		}
		entries[index] = model.TeamworkActionPolicyEntrySpec{Ordinal: uint8(index),
			OperationKind: operation, AllowedContexts: contexts, MaxResults: maxResults}
	}
	if revision.IsZero() {
		revision = model.Sum([]byte(fmt.Sprintf("acceptance-action-policy-%d-%v", offerMax, operations)))
	}
	policy, err := model.NewTeamworkActionPolicy(model.TeamworkActionPolicySpec{
		AssetRevision: revision,
		Entries:       entries})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func mustLocalOperationAuthority(t testing.TB, operation model.Operation,
	policy model.TeamworkActionPolicy,
) *LocalOperationAuthority {
	t.Helper()
	authority, err := NewLocalOperationAuthority(operation, policy)
	if err != nil {
		t.Fatal(err)
	}
	return &authority
}

func installAcceptanceChannel(t *testing.T, fixture *acceptanceFixture, signed *testkit.SignedChannel) {
	t.Helper()
	insertSignedChannelFixture(t, fixture.store.db, signed, model.TopicJoined)
	at := signed.Channel().UpdatedAt()
	members := signed.Members()
	for index, peer := range fixture.reviewers {
		member := members[index+1]
		insertSignedPeerBinding(t, fixture.store.db, fixture.channel, member,
			fmt.Sprintf("reviewer-%d", index), model.BindingPending, model.ReachabilityUnknown, at)
		epoch := member.Identity().OriginEpoch().String()
		mustExec(t, fixture.store, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
			baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at) VALUES(?,?,?,0,0,0,?)`,
			fixture.channel.String(), peer.String(), epoch, storeTime(at))
		mustExec(t, fixture.store, `UPDATE peer_bindings SET state='active' WHERE channel_id=? AND peer_id=?`,
			fixture.channel.String(), peer.String())
	}
	mustExec(t, fixture.store, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`, fixture.channel.String(),
		fixture.node.PeerID().String(), fixture.node.OriginEpoch().String(), storeTime(at))
	for _, peer := range fixture.reviewers {
		mustExec(t, fixture.store, `INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,
			origin_epoch,baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at)
			VALUES(?,?,?,?,0,0,NULL,?)`, fixture.channel.String(), peer.String(), fixture.node.PeerID().String(),
			fixture.node.OriginEpoch().String(), storeTime(at))
		mustExec(t, fixture.store, `UPDATE peer_pull_acks SET baseline_confirmed_at=?
			WHERE channel_id=? AND target_peer_id=?`, storeTime(at), fixture.channel.String(), peer.String())
	}
}

func (fixture *acceptanceFixture) reserveOffer(t *testing.T, suffix string,
	contextHash *model.Digest,
) (model.Operation, *LocalOperationAuthority) {
	t.Helper()
	runID, _ := model.ParseRunID("run-" + suffix)
	started := fixture.now.Add(-time.Minute)
	mustExec(t, fixture.store, `INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at) VALUES(?,?,'{}','test',?,'{}','{}','running',?)`,
		runID.String(), model.TeamworkProfileID().String(), string(fixture.profile.Runtime()), storeTime(started))
	operationID, _ := model.ParseOperationID("operation-" + suffix)
	leaseUntil := fixture.now.Add(10 * time.Minute)
	operation, err := model.NewOperation(model.OperationSpec{ID: operationID, ProfileID: model.TeamworkProfileID(),
		AgentRunID: runID, ClientKeyHash: model.Sum([]byte("key-" + suffix)), ContextHash: contextHash,
		Kind: model.OperationTeamworkOffer, RequestDigest: model.Sum([]byte("request-" + suffix)),
		Status: model.OperationStarted, LeaseOwner: "owner-" + suffix, LeaseUntil: &leaseUntil, CreatedAt: started})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.ReserveOperation(context.Background(), operation, fixture.now.Add(-30*time.Second)); err != nil {
		t.Fatal(err)
	}
	authority := mustLocalOperationAuthority(t, operation, fixture.policy)
	return operation, authority
}

func (fixture *acceptanceFixture) offer(t *testing.T, authority *LocalOperationAuthority,
	suffix string, reviewers []model.PeerID, artifacts []model.ArtifactRef, causes []model.EventKey,
) LocalAcceptanceSpec {
	return fixture.offerWithContent(t, authority, suffix, reviewers, artifacts, causes,
		"Review the production change")
}

func (fixture *acceptanceFixture) offerWithContent(t *testing.T,
	authority *LocalOperationAuthority, suffix string, reviewers []model.PeerID,
	artifacts []model.ArtifactRef, causes []model.EventKey, content string,
) LocalAcceptanceSpec {
	t.Helper()
	allAudience, _ := model.NewAudience(reviewers)
	snapshot, err := fixture.store.PrepareLocalAdmission(context.Background(), fixture.channel, allAudience, uint8(len(reviewers)))
	if err != nil {
		t.Fatal(err)
	}
	signer, _ := eventpkg.NewEd25519Signer(fixture.privateKey)
	factory, _ := eventpkg.NewFactory(acceptanceClock{fixture.now}, signer)
	candidate, err := eventpkg.NewOfferCandidate(content, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	items := make([]LocalAcceptanceItem, len(reviewers))
	for index, reviewer := range reviewers {
		workID, _ := model.ParseWorkID(fmt.Sprintf("work-%s-%d", suffix, index))
		ref, _ := model.NewWorkRef(fixture.node.PeerID(), workID)
		eventID, _ := model.ParseEventID(fmt.Sprintf("event-%s-%d", suffix, index))
		eventScope, _ := snapshot.EventScope(uint8(index), ref)
		audience, _ := model.NewAudience([]model.PeerID{reviewer})
		stamp, err := eventpkg.NewAdmissionStamp(eventpkg.AdmissionStampSpec{Node: snapshot.Node(), Profile: snapshot.Profile(),
			EventID: eventID, ChannelID: fixture.channel, WorkRef: ref,
			OriginSequence: eventScope.OriginSequence(), ChannelSequence: eventScope.ChannelSequence(),
			OriginMember: eventScope.OriginMember(), PublicationRoster: eventScope.PublicationRoster(), Audience: audience,
			WorkVersion: 1, Iteration: 1, Artifacts: artifacts, CausedBy: causes})
		if err != nil {
			t.Fatal(err)
		}
		bundle, err := factory.AdmitAgent(context.Background(), stamp, candidate)
		if err != nil {
			t.Fatal(err)
		}
		participants, _ := model.NewParticipantSnapshot(fixture.channel,
			eventScope.PublicationRoster().Revision(), fixture.node.PeerID(), reviewer)
		work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: ref, ChannelID: fixture.channel,
			Participants: participants, Version: 1, Iteration: 1, DeadlineUnixNano: bundle.WorkDeadlineUnixNano(),
			State: model.WorkOffered, StateData: bundle.Event().Payload(), UpdatedBy: bundle.Event().ID(), UpdatedAt: fixture.now})
		if err != nil {
			t.Fatal(err)
		}
		creation, _ := NewWorkCreation(work)
		items[index] = LocalAcceptanceItem{Publication: bundle.Publication(), Work: &creation}
	}
	return LocalAcceptanceSpec{Scope: snapshot, Items: items, Operation: authority}
}

type acceptanceClock struct{ now time.Time }

func (clock acceptanceClock) Now() time.Time { return clock.now }

func checkpointOperationRoot(t *testing.T, fixture *acceptanceFixture, operation model.Operation,
	owner string, root VerifiedArtifactRoot,
) {
	t.Helper()
	capture, _ := model.JSONFrom(struct {
		Roots []struct {
			ManifestDigest model.Digest `json:"manifest_digest"`
			RootDigest     model.Digest `json:"root_digest"`
		} `json:"roots"`
	}{[]struct {
		ManifestDigest model.Digest `json:"manifest_digest"`
		RootDigest     model.Digest `json:"root_digest"`
	}{{root.ManifestDigest, root.RootDigest}}})
	if _, err := fixture.store.CheckpointOperationCapture(context.Background(), operation.ID(), owner,
		fixture.now.Add(-time.Second), capture); err != nil {
		t.Fatal(err)
	}
}

func mustExec(t *testing.T, store *Store, query string, args ...any) {
	t.Helper()
	if _, err := store.db.Exec(query, args...); err != nil {
		t.Fatalf("fixture SQL: %v", err)
	}
}

func assertAcceptanceCounts(t *testing.T, store *Store, want []int) {
	t.Helper()
	query := `SELECT (SELECT COUNT(*) FROM events),(SELECT COUNT(*) FROM works),
		(SELECT COUNT(*) FROM work_members),(SELECT COUNT(*) FROM agent_handlings),
		(SELECT COUNT(*) FROM gossip_publications),(SELECT COUNT(*) FROM peer_deliveries),
		(SELECT COUNT(*) FROM artifact_provenance),(SELECT COUNT(*) FROM artifact_pins)`
	got := make([]int, len(want))
	if err := store.db.QueryRow(query).Scan(&got[0], &got[1], &got[2], &got[3], &got[4], &got[5], &got[6], &got[7]); err != nil {
		t.Fatal(err)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("evidence counts = %v, want %v", got, want)
		}
	}
}

func assertAcceptanceHeads(t *testing.T, store *Store, nextOrigin, publicationHead uint64) {
	t.Helper()
	var next, head uint64
	if err := store.db.QueryRow(`SELECT n.next_origin_seq,p.source_head_channel_seq FROM node n
		JOIN publication_epochs p ON p.origin_peer_id=n.peer_id AND p.origin_epoch=n.origin_epoch`).Scan(&next, &head); err != nil {
		t.Fatal(err)
	}
	if next != nextOrigin || head != publicationHead {
		t.Fatalf("sequence heads = (%d,%d), want (%d,%d)", next, head, nextOrigin, publicationHead)
	}
}
