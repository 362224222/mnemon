package selector_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/authority"
	"github.com/mnemon-dev/mnemon/harness/internal/cas"
	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

// TestR7AcceptedEventSeedsR8AndObservationReturnsOnlyThroughR7Admission
// proves the complete removable boundary. R8 consumes only an exact accepted
// R7 EventRef and produces only observational bytes. Those bytes do not enter
// the R7 world until an Agent explicitly captures them as an Artifact and
// submits an ordinary R7 Intent through admission.
func TestR7AcceptedEventSeedsR8AndObservationReturnsOnlyThroughR7Admission(t *testing.T) {
	t.Parallel()
	fixture := newCompositionFixture(t)
	seed, beforeSelector := fixture.acceptSeedOpinion()
	observation := fixture.runSelector(seed)
	afterSelector := fixture.requireSelectorIsolation(beforeSelector)
	observationEvent := fixture.admitObservation(afterSelector, observation, seed.Event())
	fixture.requireObservationProjection(observation, observationEvent, beforeSelector)
}

type compositionFixture struct {
	t          *testing.T
	ctx        context.Context
	principal  agency.AgentPrincipalID
	objects    *cas.Store
	authority  *authority.Store
	attachment authority.AttachmentProof
	descriptor selector.SelectionDescriptor
	roster     []selector.ParticipantID
}

func newCompositionFixture(t *testing.T) *compositionFixture {
	t.Helper()
	fixture := &compositionFixture{t: t, ctx: context.Background(),
		principal: mustPrincipal(t, "principal.composition")}
	objects, err := cas.Open(filepath.Join(realTempDir(t), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	fixture.objects = objects
	authorityDirectory := filepath.Join(realTempDir(t), "authority")
	mustPrivateDirectory(t, authorityDirectory)
	fixture.authority, err = authority.OpenWithArtifactVerifier(fixture.ctx,
		filepath.Join(authorityDirectory, "authority.db"), objects)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.authority.Close() })
	if err := fixture.authority.EnrollPrincipal(fixture.ctx, fixture.principal); err != nil {
		t.Fatal(err)
	}
	fixture.attachment, err = fixture.authority.IssueInteractiveAttachment(fixture.ctx,
		fixture.principal)
	if err != nil {
		t.Fatal(err)
	}
	fixture.descriptor, fixture.roster = newCompositionDescriptor(t)
	return fixture
}

func newCompositionDescriptor(t *testing.T) (selector.SelectionDescriptor, []selector.ParticipantID) {
	t.Helper()
	profile, err := selector.NewProfile(1, 1, 1, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	roster := make([]selector.ParticipantID, 5)
	for index := range roster {
		roster[index] = mustParticipant(t, "peer-"+string(rune('a'+index)))
	}
	now := time.Now().Round(0).UTC()
	descriptor, err := selector.NewSelectionDescriptor(
		agency.Sum([]byte("which candidate should remain active?")),
		agency.Sum([]byte("candidate A")), agency.Sum([]byte("candidate B")),
		roster, profile, now.Add(-time.Second), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return descriptor, roster
}

func (fixture *compositionFixture) acceptSeedOpinion() (selector.AcceptedSeedOpinion,
	authority.BoundView,
) {
	t := fixture.t
	descriptorDigest, opinion := fixture.captureSeedArtifacts()
	seedRequest, receipt := fixture.admitSeedArtifacts(descriptorDigest, opinion)
	parsedDescriptor, parsedOpinion := fixture.readSeedArtifacts(descriptorDigest, opinion)
	seed, err := selector.BindAcceptedSeedOpinion(seedRequest, receipt, parsedDescriptor,
		parsedOpinion)
	if err != nil {
		t.Fatal(err)
	}
	// This projection freezes all R7 domain state before R8 is invoked. The
	// accepted seed Event has created exactly one local responsibility.
	beforeSelector := mustCurrent(t, fixture.ctx, fixture.authority, fixture.attachment,
		"operation.current.before-selector")
	beforeProjection := decodeAgentView(t, beforeSelector)
	if beforeProjection.Current == nil || len(beforeProjection.References) != 0 {
		t.Fatalf("pre-selector View = current:%v references:%d",
			beforeProjection.Current != nil, len(beforeProjection.References))
	}
	return seed, beforeSelector
}

func (fixture *compositionFixture) captureSeedArtifacts() (agency.Digest, selector.SeedOpinion) {
	t := fixture.t
	descriptorDigest := fixture.captureArtifact(fixture.descriptor.CanonicalBytes())
	if descriptorDigest != fixture.descriptor.ID().Digest() {
		t.Fatalf("captured descriptor digest = %s, want %s",
			descriptorDigest, fixture.descriptor.ID())
	}
	opinion, err := selector.NewSeedOpinion(fixture.descriptor.ID(), selector.PreferenceA)
	if err != nil {
		t.Fatal(err)
	}
	if digest := fixture.captureArtifact(opinion.CanonicalBytes()); digest != opinion.Digest() {
		t.Fatalf("captured seed opinion digest = %s, want %s", digest, opinion.Digest())
	}
	return descriptorDigest, opinion
}

func (fixture *compositionFixture) admitSeedArtifacts(descriptorDigest agency.Digest,
	opinion selector.SeedOpinion,
) (agency.BoundIntent, agency.Receipt) {
	t := fixture.t
	rootView := mustCurrent(t, fixture.ctx, fixture.authority, fixture.attachment,
		"operation.current.root")
	seedOperation := mustOperation(t, "operation.selection.seed")
	descriptorInput, err := agency.NewArtifactCandidate(
		mustHandle(t, "candidate.selection.descriptor"))
	if err != nil {
		t.Fatal(err)
	}
	opinionInput, err := agency.NewArtifactCandidate(mustHandle(t, "candidate.selection.seed"))
	if err != nil {
		t.Fatal(err)
	}
	seedIntent := mustIntent(t, agency.IntentSpec{
		Kind:        mustLabel(t, "selection.seed"),
		Payload:     mustPayload(t, "local selection opinion"),
		Consequence: agency.ConsequenceCreateHandlings,
		Successors:  []agency.TargetRef{agency.SelfTarget()},
		Artifacts:   []agency.ArtifactInput{descriptorInput, opinionInput},
	})
	descriptorCandidate, err := agency.NewCapturedCandidate(seedOperation, descriptorInput,
		descriptorDigest)
	if err != nil {
		t.Fatal(err)
	}
	opinionCandidate, err := agency.NewCapturedCandidate(seedOperation, opinionInput,
		opinion.Digest())
	if err != nil {
		t.Fatal(err)
	}
	seedRequest, err := rootView.Bind(seedIntent, seedOperation,
		[]agency.CapturedCandidate{descriptorCandidate, opinionCandidate})
	if err != nil {
		t.Fatal(err)
	}
	seedAdmission, err := fixture.authority.Admit(fixture.ctx, fixture.attachment, seedRequest)
	if err != nil {
		t.Fatal(err)
	}
	receipt := acceptedReceipt(t, seedAdmission)
	return seedRequest, receipt
}

func (fixture *compositionFixture) readSeedArtifacts(descriptorDigest agency.Digest,
	opinion selector.SeedOpinion,
) (selector.SelectionDescriptor, selector.SeedOpinion) {
	t := fixture.t
	descriptorBytes, err := fixture.objects.Read(fixture.ctx, descriptorDigest,
		selector.MaxDescriptorBytes)
	if err != nil || agency.Sum(descriptorBytes) != descriptorDigest {
		t.Fatalf("read exact descriptor Artifact = bytes:%d digest:%s error:%v",
			len(descriptorBytes), agency.Sum(descriptorBytes), err)
	}
	parsedDescriptor, err := selector.ParseSelectionDescriptorCanonical(descriptorBytes)
	if err != nil {
		t.Fatal(err)
	}
	seedBytes, err := fixture.objects.Read(fixture.ctx, opinion.Digest(),
		selector.MaxSeedOpinionCanonicalBytes)
	if err != nil || agency.Sum(seedBytes) != opinion.Digest() {
		t.Fatalf("read exact seed opinion Artifact = bytes:%d digest:%s error:%v",
			len(seedBytes), agency.Sum(seedBytes), err)
	}
	parsedOpinion, err := selector.ParseSeedOpinionCanonical(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	return parsedDescriptor, parsedOpinion
}

func (fixture *compositionFixture) runSelector(seed selector.AcceptedSeedOpinion) selector.PreferenceObservation {
	t := fixture.t
	selectorDirectory := filepath.Join(realTempDir(t), "selector")
	mustPrivateDirectory(t, selectorDirectory)
	selectorStore, err := selector.OpenStore(fixture.ctx, filepath.Join(selectorDirectory, "selector.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = selectorStore.Close() })
	created, err := selectorStore.CreateOwnerSelection(fixture.ctx, fixture.descriptor,
		fixture.roster[0])
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := selectorStore.SeedSelection(fixture.ctx, created.Descriptor().ID(), seed)
	if err != nil {
		t.Fatal(err)
	}
	storedSeed, present := seeded.Seed()
	if !present || storedSeed.Event() != seed.Event() || storedSeed.Principal() != fixture.principal ||
		storedSeed.SelectionID() != fixture.descriptor.ID() {
		t.Fatalf("R8 seed lost exact R7 provenance: %#v", storedSeed)
	}
	pending, err := selectorStore.FreezeRound(fixture.ctx, fixture.descriptor.ID())
	if err != nil {
		t.Fatal(err)
	}
	peer := pending.Sample()[0]
	wireVote, err := selector.NewSampleVote(fixture.descriptor.ID(), pending.Query().Round(),
		pending.Query().Nonce(), selector.PreferenceA, peer)
	if err != nil {
		t.Fatal(err)
	}
	vote, err := selector.AuthenticateSampleVote(peer, wireVote)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := selectorStore.ApplyObservations(fixture.ctx, pending,
		[]selector.AuthenticatedVote{vote})
	if err != nil {
		t.Fatal(err)
	}
	observation, present := observed.Observation()
	if !present || observation.SelectionID() != fixture.descriptor.ID() ||
		observation.Result() != selector.ObservationThresholdReached {
		t.Fatalf("selector observation = present:%v selection:%s result:%s",
			present, observation.SelectionID(), observation.Result())
	}
	return observation
}

func (fixture *compositionFixture) requireSelectorIsolation(beforeSelector authority.BoundView) authority.BoundView {
	t := fixture.t
	// Settling R8 alone changes no R7 Handling or Reference. Current itself is
	// an R7 machine operation, so the per-request View handle is deliberately
	// removed before comparing the complete public world projection.
	afterSelector := mustCurrent(t, fixture.ctx, fixture.authority, fixture.attachment,
		"operation.current.after-selector")
	if before, after := domainProjection(t, beforeSelector), domainProjection(t, afterSelector); !reflect.DeepEqual(before, after) {
		t.Fatalf("R8 observation mutated R7 before Intent\nbefore=%v\nafter=%v", before, after)
	}
	return afterSelector
}

func (fixture *compositionFixture) admitObservation(afterSelector authority.BoundView,
	observation selector.PreferenceObservation, seedEvent agency.EventRef,
) agency.EventRef {
	t := fixture.t
	observationBytes := observation.CanonicalBytes()
	observationDigest := observation.Digest()
	fixture.captureArtifact(observationBytes)

	afterProjection := decodeAgentView(t, afterSelector)
	seedHandle := mustCurrentHandle(t, afterProjection)
	observationOperation := mustOperation(t, "operation.selection.observation.publish")
	candidateHandle := mustHandle(t, "candidate.selection.observation")
	candidateInput, err := agency.NewArtifactCandidate(candidateHandle)
	if err != nil {
		t.Fatal(err)
	}
	observationIntent := mustIntent(t, agency.IntentSpec{
		Kind:             mustLabel(t, "selection.preference.observed"),
		Payload:          mustPayload(t, "bounded local preference observation"),
		Consequence:      agency.ConsequencePublishReference,
		ReferenceKey:     mustReferenceKey(t, "selection.preference.current"),
		Artifacts:        []agency.ArtifactInput{candidateInput},
		CausationHandles: []agency.OpaqueHandle{seedHandle},
	})
	candidate, err := agency.NewCapturedCandidate(observationOperation, candidateInput,
		observationDigest)
	if err != nil {
		t.Fatal(err)
	}
	observationRequest, err := afterSelector.Bind(observationIntent, observationOperation,
		[]agency.CapturedCandidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	if got := observationRequest.Causation(); len(got) != 1 || got[0] != seedEvent {
		t.Fatalf("observation Intent causation = %v, want exact seed %v", got, seedEvent)
	}
	if got := observationRequest.Artifacts(); len(got) != 1 || got[0] != observationDigest {
		t.Fatalf("observation Intent Artifacts = %v, want %s", got, observationDigest)
	}
	observationAdmission, err := fixture.authority.Admit(fixture.ctx, fixture.attachment,
		observationRequest)
	if err != nil {
		t.Fatal(err)
	}
	return acceptedEvent(t, observationAdmission)
}

func (fixture *compositionFixture) captureArtifact(content []byte) agency.Digest {
	fixture.t.Helper()
	digest := agency.Sum(content)
	put, err := fixture.objects.Put(fixture.ctx, digest, content)
	if err != nil {
		fixture.t.Fatal(err)
	}
	if put.Digest != digest || put.Size != int64(len(content)) {
		fixture.t.Fatalf("captured Artifact = %#v", put)
	}
	verified, err := authority.VerifyArtifact(content, time.Now())
	if err != nil {
		fixture.t.Fatal(err)
	}
	if err := fixture.authority.CatalogArtifact(fixture.ctx, verified); err != nil {
		fixture.t.Fatal(err)
	}
	return digest
}

func (fixture *compositionFixture) requireObservationProjection(observation selector.PreferenceObservation,
	observationEvent agency.EventRef, beforeSelector authority.BoundView,
) {
	t := fixture.t
	observationDigest := observation.Digest()
	beforeProjection := decodeAgentView(t, beforeSelector)
	fresh := mustCurrent(t, fixture.ctx, fixture.authority, fixture.attachment,
		"operation.current.observation-visible")
	freshProjection := decodeAgentView(t, fresh)
	if freshProjection.Current == nil || !reflect.DeepEqual(beforeProjection.Current, freshProjection.Current) {
		t.Fatal("publishing the observation Reference unexpectedly changed the current Handling")
	}
	if len(freshProjection.References) != 1 {
		t.Fatalf("fresh View References = %d, want 1", len(freshProjection.References))
	}
	reference := freshProjection.References[0].Facts
	if reference.Key != "selection.preference.current" || reference.State != "active" ||
		reference.Artifact == nil || reference.Artifact.Digest != observationDigest.String() {
		t.Fatalf("fresh observation Reference = %#v", reference)
	}
	artifactHandle := mustHandle(t, reference.Artifact.Handle)
	if resolved, err := fresh.ResolveOfferedArtifact(artifactHandle); err != nil || resolved != observationDigest {
		t.Fatalf("resolve fresh observation Artifact = %s, %v", resolved, err)
	}
	if !slices.Contains(freshProjection.ProvenanceHandles, reference.Head) {
		t.Fatalf("fresh View does not offer Reference head %q as provenance", reference.Head)
	}

	// Binding, without admitting, proves that the fresh opaque provenance
	// handle resolves to the exact accepted Event that published the
	// observation. No selector type or field gains R7 authority in this step.
	proofIntent := mustIntent(t, agency.IntentSpec{
		Kind:             mustLabel(t, "selection.provenance.inspect"),
		Payload:          mustPayload(t, "prove exact observation lineage"),
		Consequence:      agency.ConsequenceAdvanceHandling,
		SubjectHandling:  mustCurrentHandle(t, freshProjection),
		CausationHandles: []agency.OpaqueHandle{mustHandle(t, reference.Head)},
	})
	proofRequest, err := fresh.Bind(proofIntent,
		mustOperation(t, "operation.selection.provenance.inspect"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := proofRequest.Causation(); len(got) != 1 || got[0] != observationEvent {
		t.Fatalf("fresh View provenance = %v, want exact observation Event %v",
			got, observationEvent)
	}
}

type agentViewProjection struct {
	View    string `json:"view"`
	Current *struct {
		Facts struct {
			Handle string `json:"handle"`
		} `json:"facts"`
		Semantic struct {
			Kind    string `json:"kind"`
			Payload string `json:"payload"`
		} `json:"semantic"`
	} `json:"current"`
	References []struct {
		Facts struct {
			Key      string `json:"key"`
			Head     string `json:"head"`
			State    string `json:"state"`
			Artifact *struct {
				Handle string `json:"handle"`
				Digest string `json:"digest"`
			} `json:"artifact"`
		} `json:"facts"`
	} `json:"references"`
	Targets           []string         `json:"targets"`
	AllowedIntents    []map[string]any `json:"allowed_intents"`
	ProvenanceHandles []string         `json:"provenance_handles"`
}

func decodeAgentView(t *testing.T, view authority.BoundView) agentViewProjection {
	t.Helper()
	var projection agentViewProjection
	if err := json.Unmarshal(view.AgentView().CanonicalJSON(), &projection); err != nil {
		t.Fatal(err)
	}
	return projection
}

func domainProjection(t *testing.T, view authority.BoundView) agentViewProjection {
	t.Helper()
	projection := decodeAgentView(t, view)
	projection.View = ""
	return projection
}

func mustCurrent(t *testing.T, ctx context.Context, store *authority.Store,
	proof authority.AttachmentProof, operationValue string,
) authority.BoundView {
	t.Helper()
	operation, err := authority.NewCurrentOperation(mustOperation(t, operationValue))
	if err != nil {
		t.Fatal(err)
	}
	view, err := store.Current(ctx, proof, operation)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func acceptedEvent(t *testing.T, result authority.AdmissionResult) agency.EventRef {
	t.Helper()
	receipt := acceptedReceipt(t, result)
	event, present := receipt.Event()
	if !present {
		t.Fatal("accepted Receipt has no Event")
	}
	return event
}

func acceptedReceipt(t *testing.T, result authority.AdmissionResult) agency.Receipt {
	t.Helper()
	if result.Outcome() != agency.ReceiptOutcomeAccepted || result.Replayed() {
		t.Fatalf("admission = outcome:%s replayed:%v", result.Outcome(), result.Replayed())
	}
	receipt, err := agency.ParseReceiptCanonicalJSON(result.ReceiptJSON())
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func mustPrivateDirectory(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
}

func realTempDir(t *testing.T) string {
	t.Helper()
	path, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func mustPrincipal(t *testing.T, value string) agency.AgentPrincipalID {
	t.Helper()
	result, err := agency.NewAgentPrincipalID(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustParticipant(t *testing.T, value string) selector.ParticipantID {
	t.Helper()
	result, err := selector.NewParticipantID(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustOperation(t *testing.T, value string) agency.OperationKey {
	t.Helper()
	result, err := agency.NewOperationKey(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustLabel(t *testing.T, value string) agency.SemanticLabel {
	t.Helper()
	result, err := agency.NewSemanticLabel(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustPayload(t *testing.T, value string) agency.SemanticPayload {
	t.Helper()
	result, err := agency.NewSemanticPayload(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustIntent(t *testing.T, spec agency.IntentSpec) agency.AgentIntent {
	t.Helper()
	result, err := agency.NewAgentIntent(spec)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustReferenceKey(t *testing.T, value string) agency.ReferenceKey {
	t.Helper()
	result, err := agency.NewReferenceKey(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustHandle(t *testing.T, value string) agency.OpaqueHandle {
	t.Helper()
	result, err := agency.NewOpaqueHandle(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustCurrentHandle(t *testing.T, projection agentViewProjection) agency.OpaqueHandle {
	t.Helper()
	if projection.Current == nil {
		t.Fatal("View has no current Handling")
	}
	return mustHandle(t, projection.Current.Facts.Handle)
}
