package authority

import (
	"bytes"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestReferenceOutcomeProjectionCountsOnlyDirectTerminalCitations(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:reference-outcomes")
	publishTestReference(t, fixture, "guide.outcomes", "outcome guide v1")

	terminalWithReferences(t, fixture, "completed", agency.ConsequenceResolveCompleted,
		[]string{"guide.outcomes"}, "", true)
	terminalWithReferences(t, fixture, "declined", agency.ConsequenceResolveDeclined,
		[]string{"guide.outcomes"}, "", false)
	terminalWithReferences(t, fixture, "unresolved", agency.ConsequenceResolveUnresolved,
		[]string{"guide.outcomes"}, "", false)
	// Causation and correlation can cite the same exact head. It remains one
	// terminal use, not two votes for an outcome.
	terminalWithReferences(t, fixture, "deduplicated", agency.ConsequenceResolveUnresolved,
		[]string{"guide.outcomes"}, "guide.outcomes", false)

	got := referenceOutcomeFacts(t, fixture.current(t), "guide.outcomes")
	if got != (outcomeFacts{Completed: 1, Declined: 1, Unresolved: 2}) {
		t.Fatalf("terminal outcomes = %#v", got)
	}
}

func TestReferenceOutcomeProjectionDoesNotInferAcrossHandlingHistory(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:reference-no-inheritance")
	publishTestReference(t, fixture, "guide.no-inheritance", "guide v1")

	rootView := fixture.current(t)
	head := referenceHeadHandle(t, rootView, "guide.no-inheritance")
	root := mustIntent(t, agency.IntentSpec{Kind: mustLabel(t, "work.request"),
		Payload: mustPayload(t, "root cites a guide"), Consequence: agency.ConsequenceCreateHandlings,
		Successors: []agency.TargetRef{agency.SelfTarget()}, CausationHandles: []agency.OpaqueHandle{head}})
	bound, err := rootView.Bind(root, mustOperation(t, "operation:no-inheritance-root"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := fixture.store.Admit(fixture.ctx, fixture.proof, bound); err != nil {
		t.Fatal(err)
	} else {
		requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	}
	terminal := subjectRequest(t, fixture.current(t), "operation:no-inheritance-terminal",
		agency.ConsequenceResolveUnresolved, "the terminal Event does not cite the guide", nil)
	if result, err := fixture.store.Admit(fixture.ctx, fixture.proof, terminal); err != nil {
		t.Fatal(err)
	} else {
		requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	}
	if got := referenceOutcomeFacts(t, fixture.current(t), "guide.no-inheritance"); got != (outcomeFacts{}) {
		t.Fatalf("inherited outcome = %#v, want zero", got)
	}
}

func TestReferenceOutcomeProjectionUsesExactHeadAndRebuilds(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:reference-outcome-rebuild")
	publishTestReference(t, fixture, "guide.rebuild", "guide v1")

	frozenOperation, err := NewCurrentOperation(mustOperation(t, "operation:outcome-frozen"))
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := fixture.store.Current(fixture.ctx, fixture.proof, frozenOperation)
	if err != nil {
		t.Fatal(err)
	}
	terminalWithReferences(t, fixture, "rebuild-completed", agency.ConsequenceResolveCompleted,
		[]string{"guide.rebuild"}, "", true)
	if got := referenceOutcomeFacts(t, fixture.current(t), "guide.rebuild"); got.Completed != 1 {
		t.Fatalf("fresh View completed count = %d, want 1", got.Completed)
	}
	replayed, err := fixture.store.ReplayCurrent(fixture.ctx, fixture.proof, frozenOperation)
	if err != nil {
		t.Fatal(err)
	}
	if got := referenceOutcomeFacts(t, replayed, "guide.rebuild"); got != (outcomeFacts{}) ||
		!bytes.Equal(replayed.AgentView().CanonicalJSON(), frozen.AgentView().CanonicalJSON()) {
		t.Fatal("frozen Current replay incorporated a later outcome")
	}

	before := rawP05Table(t, fixture.store, "reference_outcome_projection")
	if err := fixture.store.rebuildReferenceOutcomeProjection(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	after := rawP05Table(t, fixture.store, "reference_outcome_projection")
	if !bytes.Equal(before, after) {
		t.Fatalf("rebuilt projection differs: before=%s after=%s",
			agency.Sum(before).String(), agency.Sum(after).String())
	}
	drop := installP05Fault(t, fixture.store, `CREATE TEMP TRIGGER p05_fault
		AFTER INSERT ON reference_outcome_projection
		BEGIN SELECT RAISE(ABORT, 'fault: outcome rebuild'); END`)
	if err := fixture.store.rebuildReferenceOutcomeProjection(fixture.ctx); err == nil {
		t.Fatal("faulted outcome rebuild unexpectedly succeeded")
	}
	if got := rawP05Table(t, fixture.store, "reference_outcome_projection"); !bytes.Equal(before, got) {
		t.Fatal("faulted rebuild did not restore the previous projection")
	}
	drop()

	second := fixture.catalog(t, "guide v2")
	supersede := referenceRequest(t, fixture.current(t), "operation:outcome-supersede",
		agency.ConsequenceSupersedeReference, "guide.rebuild", &second)
	if result, err := fixture.store.Admit(fixture.ctx, fixture.proof, supersede); err != nil {
		t.Fatal(err)
	} else {
		requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	}
	if got := referenceOutcomeFacts(t, fixture.current(t), "guide.rebuild"); got != (outcomeFacts{}) {
		t.Fatalf("new exact head inherited old outcomes: %#v", got)
	}

	retract := referenceRequest(t, fixture.current(t), "operation:outcome-retract",
		agency.ConsequenceRetractReference, "guide.rebuild", nil)
	if result, err := fixture.store.Admit(fixture.ctx, fixture.proof, retract); err != nil {
		t.Fatal(err)
	} else {
		requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	}
	view := fixture.current(t)
	if state := referenceState(t, view, "guide.rebuild"); state != "retracted" {
		t.Fatalf("Reference state = %q, want retracted", state)
	}
	if got := referenceOutcomeFacts(t, view, "guide.rebuild"); got != (outcomeFacts{}) {
		t.Fatalf("retraction head inherited predecessor outcomes: %#v", got)
	}
}

func TestReferenceOutcomeProjectionAttributesOneTerminalEventToEachExactHead(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:reference-multiple-outcomes")
	publishTestReference(t, fixture, "guide.first", "first guide")
	publishTestReference(t, fixture, "guide.second", "second guide")
	terminalWithReferences(t, fixture, "multiple", agency.ConsequenceResolveDeclined,
		[]string{"guide.first", "guide.second"}, "", false)
	for _, key := range []string{"guide.first", "guide.second"} {
		if got := referenceOutcomeFacts(t, fixture.current(t), key); got.Declined != 1 {
			t.Fatalf("%s outcomes = %#v, want one declined", key, got)
		}
	}
}

func TestReferenceOutcomeProjectionRequiresLocalUseAfterPeerReadmission(t *testing.T) {
	fixture := newPeerRoundTripFixture(t)
	delivery := fixture.admitOrigin(t)
	fixture.admitReceiver(t, delivery)
	publishTestReference(t, fixture.receiver, "guide.peer-local", "receiver-local guide")
	if got := referenceOutcomeFacts(t, fixture.receiver.current(t), "guide.peer-local"); got != (outcomeFacts{}) {
		t.Fatalf("remote provenance became a local outcome: %#v", got)
	}

	view := fixture.receiver.current(t)
	head := referenceHeadHandle(t, view, "guide.peer-local")
	intent := mustIntent(t, agency.IntentSpec{Kind: mustLabel(t, "work.peer-result"),
		Payload:     mustPayload(t, "decline after local consideration"),
		Consequence: agency.ConsequenceResolveDeclined, SubjectHandling: currentSubjectHandle(t, view),
		CausationHandles: []agency.OpaqueHandle{head}})
	request, err := view.Bind(intent, mustOperation(t, "operation:peer-local-outcome"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := fixture.receiver.store.Admit(fixture.receiver.ctx, fixture.receiver.proof,
		request); err != nil {
		t.Fatal(err)
	} else {
		requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	}
	if got := referenceOutcomeFacts(t, fixture.receiver.current(t), "guide.peer-local"); got.Declined != 1 {
		t.Fatalf("local terminal outcome after peer re-admission = %#v", got)
	}
}

func TestReferenceOutcomeProjectionFaultRollsBackFirstIncrement(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:reference-outcome-insert-fault")
	publishTestReference(t, fixture, "guide.insert-fault", "insert fault guide")
	request := bindTerminalWithReferences(t, fixture, "insert-fault",
		agency.ConsequenceResolveCompleted, []string{"guide.insert-fault"}, "", true)
	before := snapshotP05Authority(t, fixture.store)
	drop := installP05Fault(t, fixture.store, `CREATE TEMP TRIGGER p05_fault
		AFTER INSERT ON reference_outcome_projection
		BEGIN SELECT RAISE(ABORT, 'fault: outcome insert'); END`)
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, request); err == nil {
		t.Fatal("faulted outcome insert unexpectedly succeeded")
	}
	requireP05Snapshot(t, fixture.store, before)
	drop()
	requireExactAdmissionReplay(t, fixture, request)
}

func TestReferenceOutcomeProjectionFaultRollsBackIncrement(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:reference-outcome-update-fault")
	publishTestReference(t, fixture, "guide.update-fault", "update fault guide")
	terminalWithReferences(t, fixture, "update-first", agency.ConsequenceResolveUnresolved,
		[]string{"guide.update-fault"}, "", false)
	request := bindTerminalWithReferences(t, fixture, "update-fault",
		agency.ConsequenceResolveUnresolved, []string{"guide.update-fault"}, "", false)
	before := snapshotP05Authority(t, fixture.store)
	drop := installP05Fault(t, fixture.store, `CREATE TEMP TRIGGER p05_fault
		AFTER UPDATE ON reference_outcome_projection
		BEGIN SELECT RAISE(ABORT, 'fault: outcome update'); END`)
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, request); err == nil {
		t.Fatal("faulted outcome update unexpectedly succeeded")
	}
	requireP05Snapshot(t, fixture.store, before)
	drop()
	requireExactAdmissionReplay(t, fixture, request)
}

func TestReferenceOutcomeProjectionOverflowRollsBackAdmission(t *testing.T) {
	fixture := newAuthorityFixture(t, "principal:reference-outcome-overflow")
	publishTestReference(t, fixture, "guide.overflow", "overflow guide")
	terminalWithReferences(t, fixture, "overflow-first", agency.ConsequenceResolveCompleted,
		[]string{"guide.overflow"}, "", true)
	request := bindTerminalWithReferences(t, fixture, "overflow-second",
		agency.ConsequenceResolveCompleted, []string{"guide.overflow"}, "", true)
	if _, err := fixture.store.db.Exec(`UPDATE reference_outcome_projection
		SET completed_count = ?`, maxReferenceOutcomeCount); err != nil {
		t.Fatal(err)
	}
	before := snapshotP05Authority(t, fixture.store)
	if _, err := fixture.store.Admit(fixture.ctx, fixture.proof, request); err == nil {
		t.Fatal("overflowing outcome admission unexpectedly succeeded")
	}
	requireP05Snapshot(t, fixture.store, before)
}

func publishTestReference(t *testing.T, fixture *authorityFixture, key, content string) {
	t.Helper()
	digest := fixture.catalog(t, content)
	request := referenceRequest(t, fixture.current(t), "operation:publish:"+key,
		agency.ConsequencePublishReference, key, &digest)
	result, err := fixture.store.Admit(fixture.ctx, fixture.proof, request)
	if err != nil {
		t.Fatal(err)
	}
	requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
}

func terminalWithReferences(t *testing.T, fixture *authorityFixture, suffix string,
	consequence agency.Consequence, causationKeys []string, correlationKey string, artifact bool,
) {
	t.Helper()
	bound := bindTerminalWithReferences(t, fixture, suffix, consequence, causationKeys,
		correlationKey, artifact)
	result, err := fixture.store.Admit(fixture.ctx, fixture.proof, bound)
	if err != nil {
		t.Fatal(err)
	}
	requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
}

func bindTerminalWithReferences(t *testing.T, fixture *authorityFixture, suffix string,
	consequence agency.Consequence, causationKeys []string, correlationKey string, artifact bool,
) agency.BoundIntent {
	t.Helper()
	root := rootRequest(t, fixture.current(t), "operation:root:"+suffix, "work "+suffix)
	if result, err := fixture.store.Admit(fixture.ctx, fixture.proof, root); err != nil {
		t.Fatal(err)
	} else {
		requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	}
	view := fixture.current(t)
	spec := agency.IntentSpec{Kind: mustLabel(t, "work.terminal"), Payload: mustPayload(t, suffix),
		Consequence: consequence, SubjectHandling: currentSubjectHandle(t, view)}
	for _, key := range causationKeys {
		spec.CausationHandles = append(spec.CausationHandles, referenceHeadHandle(t, view, key))
	}
	if correlationKey != "" {
		spec.CorrelationHandle = referenceHeadHandle(t, view, correlationKey)
	}
	operation := mustOperation(t, "operation:terminal:"+suffix)
	var candidates []agency.CapturedCandidate
	if artifact {
		digest := fixture.catalog(t, "verified artifact "+suffix)
		handle := mustHandle(t, "candidate:terminal:"+suffix)
		input, err := agency.NewArtifactCandidate(handle)
		if err != nil {
			t.Fatal(err)
		}
		spec.Artifacts = []agency.ArtifactInput{input}
		candidate, err := agency.NewCapturedCandidate(operation, input, digest)
		if err != nil {
			t.Fatal(err)
		}
		candidates = []agency.CapturedCandidate{candidate}
	}
	bound, err := view.Bind(mustIntent(t, spec), operation, candidates)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

type outcomeFacts struct {
	Completed  int64
	Declined   int64
	Unresolved int64
}

func referenceOutcomeFacts(t *testing.T, view BoundView, key string) outcomeFacts {
	t.Helper()
	for _, reference := range decodePublicView(t, view).References {
		if reference.Facts.Key != key || reference.Facts.TerminalOutcomes == nil {
			continue
		}
		return outcomeFacts{Completed: reference.Facts.TerminalOutcomes.Completed,
			Declined:   reference.Facts.TerminalOutcomes.Declined,
			Unresolved: reference.Facts.TerminalOutcomes.Unresolved}
	}
	return outcomeFacts{}
}

func referenceState(t *testing.T, view BoundView, key string) string {
	t.Helper()
	for _, reference := range decodePublicView(t, view).References {
		if reference.Facts.Key == key {
			return reference.Facts.State
		}
	}
	t.Fatalf("View has no Reference %q", key)
	return ""
}
