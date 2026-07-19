package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestCommitLocalAcceptanceRequiresExactOperationArtifactProjection(t *testing.T) {
	t.Run("normal checkpoint projection", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		operation, authority := fixture.reserveOffer(t, "artifact-projection-normal", nil)
		root := verifiedRoot(t, "artifact-projection-normal-root",
			`{"entries":[],"kind":"review","total_bytes":0}`, 0)
		if _, err := fixture.store.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		checkpointOperationRoot(t, fixture, operation, authority.LeaseOwner, root)
		produced, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
		spec := fixture.offer(t, authority, "artifact-projection-normal", fixture.reviewers,
			[]model.ArtifactRef{produced}, nil)
		if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
			fixture.now.Add(time.Second)); err != nil {
			t.Fatalf("accept exact operation Artifact projection: %v", err)
		}
		assertAcceptanceCounts(t, fixture.store, []int{1, 1, 2, 0, 1, 1, 1, 3})
		assertOperationStatus(t, fixture.store, operation.ID(), model.OperationCommitted)
	})

	for _, test := range []struct {
		name   string
		suffix string
		tamper func(*testing.T, *acceptanceFixture, model.Operation, VerifiedArtifactRoot)
	}{
		{
			name:   "missing projection",
			suffix: "artifact-projection-missing",
			tamper: func(t *testing.T, fixture *acceptanceFixture, operation model.Operation,
				root VerifiedArtifactRoot,
			) {
				setAcceptanceOperationCaptureDirectly(t, fixture, operation, root)
			},
		},
		{
			name:   "different projection",
			suffix: "artifact-projection-different",
			tamper: func(t *testing.T, fixture *acceptanceFixture, operation model.Operation,
				root VerifiedArtifactRoot,
			) {
				mustExec(t, fixture.store, `INSERT INTO operation_artifact_roots(
					operation_id,root_digest,manifest_digest) VALUES(?,?,?)`, operation.ID().String(),
					root.RootDigest.String(), model.Sum([]byte("different-projection-manifest")).Bytes())
				setAcceptanceOperationCaptureDirectly(t, fixture, operation, root)
			},
		},
		{
			name:   "extra projection",
			suffix: "artifact-projection-extra",
			tamper: func(t *testing.T, fixture *acceptanceFixture, operation model.Operation,
				root VerifiedArtifactRoot,
			) {
				mustExec(t, fixture.store, `INSERT INTO operation_artifact_roots(
					operation_id,root_digest,manifest_digest) VALUES(?,?,?)`, operation.ID().String(),
					root.RootDigest.String(), root.ManifestDigest.Bytes())
				mustExec(t, fixture.store, `INSERT INTO operation_artifact_roots(
					operation_id,root_digest,manifest_digest) VALUES(?,?,?)`, operation.ID().String(),
					model.Sum([]byte("extra-projection-root")).String(),
					model.Sum([]byte("extra-projection-manifest")).Bytes())
				setAcceptanceOperationCaptureDirectly(t, fixture, operation, root)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAcceptanceFixture(t, 1)
			operation, authority := fixture.reserveOffer(t, test.suffix, nil)
			root := verifiedRoot(t, test.suffix+"-root",
				`{"entries":[],"kind":"review","total_bytes":0}`, 0)
			if _, err := fixture.store.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			test.tamper(t, fixture, operation, root)
			produced, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
			spec := fixture.offer(t, authority, test.suffix, fixture.reviewers,
				[]model.ArtifactRef{produced}, nil)
			if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
				fixture.now.Add(time.Second)); !errors.Is(err, ErrOperationArtifactProjection) {
				t.Fatalf("CommitLocalAcceptance() error = %v, want projection mismatch", err)
			}
			assertAcceptanceProjectionFailure(t, fixture, operation.ID())
		})
	}

	t.Run("capture absent with preinjected projection", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		operation, authority := fixture.reserveOffer(t, "artifact-projection-without-capture", nil)
		mustExec(t, fixture.store, `INSERT INTO operation_artifact_roots(
			operation_id,root_digest,manifest_digest) VALUES(?,?,?)`, operation.ID().String(),
			model.Sum([]byte("preinjected-projection-root")).String(),
			model.Sum([]byte("preinjected-projection-manifest")).Bytes())
		spec := fixture.offer(t, authority, "artifact-projection-without-capture",
			fixture.reviewers, nil, nil)
		if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
			fixture.now.Add(time.Second)); !errors.Is(err, ErrOperationArtifactProjection) {
			t.Fatalf("CommitLocalAcceptance() error = %v, want projection mismatch", err)
		}
		assertAcceptanceProjectionFailure(t, fixture, operation.ID())
	})
}

func setAcceptanceOperationCaptureDirectly(t *testing.T, fixture *acceptanceFixture,
	operation model.Operation, root VerifiedArtifactRoot,
) {
	t.Helper()
	capture := operationCaptureJSON(t, []captureRoot{{
		RootDigest: root.RootDigest, ManifestDigest: root.ManifestDigest,
	}})
	mustExec(t, fixture.store, `UPDATE operations SET capture_json=? WHERE operation_id=?`,
		capture.Bytes(), operation.ID().String())
}

func assertAcceptanceProjectionFailure(t *testing.T, fixture *acceptanceFixture,
	operation model.OperationID,
) {
	t.Helper()
	assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
	assertAcceptanceHeads(t, fixture.store, 1, 0)
	assertOperationStatus(t, fixture.store, operation, model.OperationStarted)
	var resultIsNull, finishedIsNull int
	if err := fixture.store.db.QueryRow(`SELECT result_json IS NULL,finished_at IS NULL
		FROM operations WHERE operation_id=?`, operation.String()).Scan(&resultIsNull, &finishedIsNull); err != nil ||
		resultIsNull != 1 || finishedIsNull != 1 {
		t.Fatalf("operation terminal fields after projection rejection = (%d,%d,%v)",
			resultIsNull, finishedIsNull, err)
	}
}

func TestValidateOperationEventsClosesControllerBypass(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	allowed := []struct {
		typeValue model.EventType
		payload   string
	}{
		{model.EventReviewAccepted, `{"iteration":1,"work_version":1}`},
		{model.EventReviewAcceptRejected, `{"diagnostic_code":"race","iteration":1,"work_version":1}`},
		{model.EventReviewDelivered, `{"iteration":1,"work_version":2}`},
		{model.EventReviewDeclined, `{"iteration":1,"work_version":1}`},
		{model.EventReviewExpired, `{"deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`},
		{model.EventReviewOutcome, `{"decision_ref":"d","diagnostic_code":"fallback","iteration":1,"status":"accepted","work_version":1}`},
	}
	for _, test := range allowed {
		event := eventWithCodecCause(t, newCodecEvent(t, test.typeValue, test.payload, now))
		if err := validateOperationEvents(nil, []model.Event{event}); err != nil {
			t.Errorf("controller %s error = %v", test.typeValue, err)
		}
	}
	for _, forbidden := range []model.EventType{
		model.EventReviewOffered, model.EventReviewAcceptRequested, model.EventReviewDeclineRequested,
		model.EventReviewDeliveryReady, model.EventReviewReworkRequested, model.EventReviewClosed,
		model.EventReviewCancelled,
	} {
		event := eventWithCodecCause(t, newCodecEvent(t, forbidden, `{"iteration":1,"work_version":1}`, now))
		if err := validateOperationEvents(nil, []model.Event{event}); err == nil {
			t.Errorf("controller accepted %s", forbidden)
		}
	}
	valid := eventWithCodecCause(t, newCodecEvent(t, model.EventReviewAccepted, `{"iteration":1,"work_version":1}`, now))
	if err := validateOperationEvents(nil, []model.Event{valid, valid}); err == nil {
		t.Fatal("controller accepted a batch")
	}
}

func TestValidateParticipantBindingBoundsReceiptOnlyVersions(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	offered, current := commitCausalOffer(t, fixture, "participant-receipt-version")
	current = commitCausalAcceptedWork(t, fixture, offered, current, fixture.now.Add(3*time.Second))
	if current.Version() != 2 || current.Iteration() != 1 {
		t.Fatalf("current Work = version %d iteration %d", current.Version(), current.Iteration())
	}

	tests := []struct {
		name      string
		eventType model.EventType
		payload   string
		wantErr   bool
	}{
		{"stale accept rejected", model.EventReviewAcceptRejected,
			`{"diagnostic_code":"stale","iteration":1,"work_version":1}`, false},
		{"stale outcome", model.EventReviewOutcome,
			`{"decision_ref":"decision-stale","diagnostic_code":"stale","iteration":1,"status":"rejected","work_version":1}`, false},
		{"current outcome", model.EventReviewOutcome,
			`{"decision_ref":"decision-current","diagnostic_code":"current","iteration":1,"status":"accepted","work_version":2}`, false},
		{"future receipt version", model.EventReviewOutcome,
			`{"decision_ref":"decision-future","diagnostic_code":"future","iteration":1,"status":"rejected","work_version":3}`, true},
		{"same version wrong iteration", model.EventReviewOutcome,
			`{"decision_ref":"decision-wrong-iteration","diagnostic_code":"wrong-iteration","iteration":2,"status":"rejected","work_version":2}`, true},
		{"older version future iteration", model.EventReviewOutcome,
			`{"decision_ref":"decision-future-iteration","diagnostic_code":"future-iteration","iteration":2,"status":"rejected","work_version":1}`, true},
		{"stale accepted state change", model.EventReviewAccepted,
			`{"iteration":1,"work_version":1}`, true},
		{"stale delivered state change", model.EventReviewDelivered,
			`{"iteration":1,"work_version":1}`, true},
		{"stale declined state change", model.EventReviewDeclined,
			`{"iteration":1,"work_version":1}`, true},
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := causalSuccessorEvent(t, offered, test.eventType,
				"event-participant-binding-"+string(rune('a'+index)), test.payload, offered.Key(),
				fixture.now.Add(4*time.Second))
			err := validateParticipantBinding(context.Background(), tx, LocalAcceptanceItem{}, event)
			if test.wantErr && err == nil {
				t.Fatal("binding was accepted")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("binding error = %v", err)
			}
		})
	}
}

func TestRequireExactDeliveredArtifactClosureRejectsReusableSubstitution(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	_, authority := fixture.reserveOffer(t, "delivered-closure", nil)
	offer := fixture.offer(t, authority, "delivered-closure", fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), offer,
		fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	rootA := model.Sum([]byte("delivered-source-root"))
	rootB := model.Sum([]byte("unrelated-reusable-root"))
	producedA, _ := model.NewArtifactRef(rootA, model.ArtifactProduced)
	source := insertImportedCausalEvent(t, fixture, offer.Items[0].Publication.Event(),
		model.EventReviewDeliveryReady, "event-delivered-source",
		`{"content":"ready","iteration":1,"work_version":1}`, fixture.now.Add(2*time.Second), producedA)
	base := offer.Items[0].Publication.Event()
	delivered := causalSuccessorEvent(t, base, model.EventReviewDelivered, "event-delivered-exact",
		`{"iteration":1,"work_version":1}`, source, fixture.now.Add(3*time.Second))
	spec := delivered.Spec()
	referencedA, _ := model.NewArtifactRef(rootA, model.ArtifactReferenced)
	spec.Artifacts = []model.ArtifactRef{referencedA}
	delivered, _ = model.NewEvent(spec)
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := requireExactDeliveredArtifactClosure(context.Background(), tx, delivered); err != nil {
		t.Fatalf("exact delivered closure error = %v", err)
	}
	referencedB, _ := model.NewArtifactRef(rootB, model.ArtifactReferenced)
	spec.ID, _ = model.ParseEventID("event-delivered-substitution")
	spec.Artifacts = []model.ArtifactRef{referencedB}
	substituted, _ := model.NewEvent(spec)
	if err := requireExactDeliveredArtifactClosure(context.Background(), tx, substituted); err == nil {
		t.Fatal("unrelated reusable root substituted for delivery.ready closure")
	}
}

func TestValidateOperationEventsRequiresExactActionAndExpandedSemantics(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	offer := newCodecEvent(t, model.EventReviewOffered,
		`{"content":"review","deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`, now)
	authority := &LocalOperationAuthority{Kind: model.OperationTeamworkOffer}
	if err := validateOperationEvents(authority, []model.Event{offer}); err != nil {
		t.Fatalf("offer validation error = %v", err)
	}
	changed := newCodecEvent(t, model.EventReviewOffered,
		`{"content":"different","deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`, now)
	if err := validateOperationEvents(authority, []model.Event{offer, changed}); err == nil {
		t.Fatal("expanded offer content drift was accepted")
	}
	expired := eventWithCodecCause(t, newCodecEvent(t, model.EventReviewExpired,
		`{"deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`, now))
	cancel := &LocalOperationAuthority{Kind: model.OperationTeamworkCancel}
	if err := validateOperationEvents(cancel, []model.Event{expired}); err == nil {
		t.Fatal("cancel operation was committed as expiry success")
	}
}

func eventWithCodecCause(t *testing.T, event model.Event) model.Event {
	t.Helper()
	causePeer, _ := model.ParsePeerID("peer-cause")
	causeEpoch, _ := model.ParseOriginEpoch("epoch-cause")
	causeID, _ := model.ParseEventID("event-cause")
	cause, _ := model.NewEventKey(causePeer, causeEpoch, causeID)
	spec := event.Spec()
	spec.CausedBy = []model.EventKey{cause}
	result, err := model.NewEvent(spec)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
