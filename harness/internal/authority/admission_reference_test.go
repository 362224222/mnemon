package authority

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestConcurrentReferenceCASAcceptsExactlyOneCandidate(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:reference-cas")
	view := fixture.current(t)
	firstDigest := fixture.catalog(t, "review playbook v1-a")
	secondDigest := fixture.catalog(t, "review playbook v1-b")
	first := referenceRequest(t, view, "operation:publish-a", agency.ConsequencePublishReference,
		"playbook.review", &firstDigest)
	second := referenceRequest(t, view, "operation:publish-b", agency.ConsequencePublishReference,
		"playbook.review", &secondDigest)

	results := make(chan AdmissionResult, 2)
	errors := make(chan error, 2)
	var wait sync.WaitGroup
	for _, request := range []agency.BoundIntent{first, second} {
		wait.Add(1)
		go func(candidate agency.BoundIntent) {
			defer wait.Done()
			result, err := fixture.store.Admit(fixture.ctx, fixture.proof, candidate)
			if err != nil {
				errors <- err
				return
			}
			results <- result
		}(request)
	}
	wait.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	accepted, rejected := 0, 0
	for result := range results {
		switch result.Outcome() {
		case agency.ReceiptOutcomeAccepted:
			accepted++
		case agency.ReceiptOutcomeRejected:
			rejected++
		}
	}
	if accepted != 1 || rejected != 1 {
		t.Fatalf("Reference race = accepted:%d rejected:%d", accepted, rejected)
	}
	if countRows(t, fixture.store, "active_references") != 1 ||
		countRows(t, fixture.store, "reference_lineage") != 1 {
		t.Fatal("Reference CAS created multiple accepted heads")
	}
}

func TestReferenceCanRetractThenSupersedeTombstone(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:reference-lineage")
	firstDigest := fixture.catalog(t, "playbook v1")
	publish := referenceRequest(t, fixture.current(t), "operation:publish-v1",
		agency.ConsequencePublishReference, "playbook.review", &firstDigest)
	if result, err := fixture.store.Admit(fixture.ctx, fixture.proof, publish); err != nil {
		t.Fatal(err)
	} else {
		requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	}

	retractView := fixture.current(t)
	retract := referenceRequest(t, retractView, "operation:retract-v1",
		agency.ConsequenceRetractReference, "playbook.review", nil)
	if result, err := fixture.store.Admit(fixture.ctx, fixture.proof, retract); err != nil {
		t.Fatal(err)
	} else {
		requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	}
	var state string
	if err := fixture.store.db.QueryRow(`SELECT state FROM active_references
		WHERE reference_key='playbook.review'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "retracted" {
		t.Fatalf("Reference state = %q, want retracted", state)
	}

	secondDigest := fixture.catalog(t, "playbook v2")
	supersedeView := fixture.current(t)
	supersede := referenceRequest(t, supersedeView, "operation:supersede-v2",
		agency.ConsequenceSupersedeReference, "playbook.review", &secondDigest)
	if result, err := fixture.store.Admit(fixture.ctx, fixture.proof, supersede); err != nil {
		t.Fatal(err)
	} else {
		requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	}
	var artifact string
	if err := fixture.store.db.QueryRow(`SELECT state, artifact_digest FROM active_references
		WHERE reference_key='playbook.review'`).Scan(&state, &artifact); err != nil {
		t.Fatal(err)
	}
	if state != "active" || artifact != secondDigest.String() {
		t.Fatalf("revived Reference = %s/%s", state, artifact)
	}
	if got := countRows(t, fixture.store, "reference_lineage"); got != 3 {
		t.Fatalf("Reference lineage rows = %d, want 3", got)
	}
}

func TestReferenceProjectionBoundDoesNotOfferSeventeenthPublish(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:reference-bound")
	for index := 0; index < maxProjectedReferences; index++ {
		digest := fixture.catalog(t, fmt.Sprintf("playbook %d", index))
		request := referenceRequest(t, fixture.current(t), fmt.Sprintf("operation:bound-%d", index),
			agency.ConsequencePublishReference, fmt.Sprintf("playbook.bound-%d", index), &digest)
		result, err := fixture.store.Admit(fixture.ctx, fixture.proof, request)
		if err != nil {
			t.Fatal(err)
		}
		requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	}
	view := fixture.current(t)
	if strings.Contains(string(view.AgentView().CanonicalJSON()), "reference.publish") {
		t.Fatal("Reference publish remained offered at the projection bound")
	}
	extraDigest := fixture.catalog(t, "must not become seventeenth")
	handle := mustHandle(t, "candidate:operation:bound-extra")
	input, err := agency.NewArtifactCandidate(handle)
	if err != nil {
		t.Fatal(err)
	}
	intent := mustIntent(t, agency.IntentSpec{Kind: mustLabel(t, "knowledge.playbook"),
		Payload: mustPayload(t, "extra"), Consequence: agency.ConsequencePublishReference,
		ReferenceKey: mustReferenceKey(t, "playbook.bound-extra"),
		Artifacts:    []agency.ArtifactInput{input}})
	operation := mustOperation(t, "operation:bound-extra")
	candidate, err := agency.NewCapturedCandidate(operation, input, extraDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := view.Bind(intent, operation, []agency.CapturedCandidate{candidate}); err == nil {
		t.Fatal("seventeenth Reference publish bound successfully")
	}
	if got := countRows(t, fixture.store, "active_references"); got != maxProjectedReferences {
		t.Fatalf("active References = %d, want %d", got, maxProjectedReferences)
	}
}
