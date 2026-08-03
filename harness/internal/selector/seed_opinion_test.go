package selector

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestSeedOpinionCanonicalRoundTrip(t *testing.T) {
	descriptor := seedDescriptorFixture(t, "round-trip")
	opinion := seedOpinionFixture(t, descriptor, PreferenceA)
	parsed, err := ParseSeedOpinionCanonical(opinion.CanonicalBytes())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.SelectionID() != opinion.SelectionID() ||
		parsed.Preference() != opinion.Preference() || parsed.Digest() != opinion.Digest() {
		t.Fatalf("parsed opinion = selection:%s preference:%s digest:%s",
			parsed.SelectionID(), parsed.Preference(), parsed.Digest())
	}

	invalid := []struct {
		value []byte
		want  error
	}{
		{append(opinion.CanonicalBytes(), '\n'), ErrInvalid},
		{[]byte(`{"preference":"a","selection_id":"bad","unknown":true,"version":1}`), ErrInvalid},
		{bytes.Repeat([]byte{'x'}, MaxSeedOpinionCanonicalBytes+1), ErrLimit},
	}
	for _, test := range invalid {
		if _, err := ParseSeedOpinionCanonical(test.value); !errors.Is(err, test.want) {
			t.Fatalf("ParseSeedOpinionCanonical(%q) error = %v, want %v",
				test.value, err, test.want)
		}
	}
}

func TestSelectionDescriptorCanonicalArtifactParser(t *testing.T) {
	descriptor := seedDescriptorFixture(t, "descriptor-parser")
	canonical := descriptor.CanonicalBytes()
	parsed, err := ParseSelectionDescriptorCanonical(canonical)
	if err != nil || parsed.ID() != descriptor.ID() {
		t.Fatalf("parsed descriptor = %s, error %v", parsed.ID(), err)
	}
	if _, err := ParseSelectionDescriptorCanonical(append(canonical, '\n')); !errors.Is(err, ErrInvalid) {
		t.Fatalf("noncanonical descriptor error = %v, want ErrInvalid", err)
	}
	if _, err := ParseSelectionDescriptorCanonical(bytes.Repeat([]byte{'x'},
		MaxDescriptorBytes+1)); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized descriptor error = %v, want ErrLimit", err)
	}
}

func TestBindAcceptedSeedOpinionRequiresExactAcceptedRequest(t *testing.T) {
	descriptor := seedDescriptorFixture(t, "accepted")
	opinion := seedOpinionFixture(t, descriptor, PreferenceB)
	request := seedRequestFixture(t, "accepted", descriptor.ID().Digest(), opinion.Digest())
	receipt := seedReceiptFixture(t, request, true, "accepted")
	seed, err := BindAcceptedSeedOpinion(request, receipt, descriptor, opinion)
	if err != nil {
		t.Fatal(err)
	}
	if seed.SelectionID() != opinion.SelectionID() || seed.Preference() != PreferenceB ||
		seed.Principal() != request.Attachment().Principal() || seed.Event().IsZero() {
		t.Fatalf("bound seed = %#v", seed)
	}

	rejected := seedReceiptFixture(t, request, false, "rejected")
	if _, err := BindAcceptedSeedOpinion(request, rejected, descriptor, opinion); !errors.Is(err, ErrConflict) {
		t.Fatalf("rejected binding error = %v, want ErrConflict", err)
	}
	otherRequest := seedRequestFixture(t, "other-operation", descriptor.ID().Digest(), opinion.Digest())
	if _, err := BindAcceptedSeedOpinion(otherRequest, receipt, descriptor, opinion); !errors.Is(err, ErrConflict) {
		t.Fatalf("request mismatch error = %v, want ErrConflict", err)
	}
	uncitedRequest := seedRequestFixture(t, "uncited", descriptor.ID().Digest())
	uncitedReceipt := seedReceiptFixture(t, uncitedRequest, true, "uncited")
	if _, err := BindAcceptedSeedOpinion(uncitedRequest, uncitedReceipt, descriptor,
		opinion); !errors.Is(err, ErrConflict) {
		t.Fatalf("uncited opinion error = %v, want ErrConflict", err)
	}
	wrongDescriptor := seedDescriptorFixture(t, "wrong-descriptor")
	if _, err := BindAcceptedSeedOpinion(request, receipt, wrongDescriptor,
		opinion); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched descriptor error = %v, want ErrInvalid", err)
	}
}

func seedDescriptorFixture(t testing.TB, name string) SelectionDescriptor {
	t.Helper()
	profile, err := NewProfile(1, 1, 1, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	left, err := NewParticipantID("peer-left-" + name)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewParticipantID("peer-right-" + name)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	descriptor, err := NewSelectionDescriptor(agency.Sum([]byte("question-"+name)),
		agency.Sum([]byte("candidate-a-"+name)), agency.Sum([]byte("candidate-b-"+name)),
		[]ParticipantID{left, right}, profile, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func seedOpinionFixture(t testing.TB, descriptor SelectionDescriptor,
	preference Preference,
) SeedOpinion {
	t.Helper()
	opinion, err := NewSeedOpinion(descriptor.ID(), preference)
	if err != nil {
		t.Fatal(err)
	}
	return opinion
}

func seedRequestFixture(t testing.TB, name string, digests ...agency.Digest) agency.BoundIntent {
	t.Helper()
	principal, err := agency.NewAgentPrincipalID("principal-" + name)
	if err != nil {
		t.Fatal(err)
	}
	attachmentID, err := agency.NewAttachmentID("attachment-" + name)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	attachment, err := agency.NewAttachment(attachmentID, principal, true, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	target, err := agency.ResolveLocalTarget(agency.SelfTarget(), principal)
	if err != nil {
		t.Fatal(err)
	}
	view, err := agency.NewViewAuthority(agency.MachineViewSpec{Attachment: attachment,
		Consequences: []agency.Consequence{agency.ConsequenceCreateHandlings},
		Targets:      []agency.ResolvedTarget{target}})
	if err != nil {
		t.Fatal(err)
	}
	inputs := make([]agency.ArtifactInput, len(digests))
	candidates := make([]agency.CapturedCandidate, len(digests))
	kind, err := agency.NewSemanticLabel("selection.seed")
	if err != nil {
		t.Fatal(err)
	}
	operation, err := agency.NewOperationKey("operation-" + name)
	if err != nil {
		t.Fatal(err)
	}
	for index, digest := range digests {
		handle, handleErr := agency.NewOpaqueHandle("candidate-" + name + "-" + string(rune('a'+index)))
		if handleErr != nil {
			t.Fatal(handleErr)
		}
		input, inputErr := agency.NewArtifactCandidate(handle)
		if inputErr != nil {
			t.Fatal(inputErr)
		}
		inputs[index] = input
		candidate, candidateErr := agency.NewCapturedCandidate(operation, input, digest)
		if candidateErr != nil {
			t.Fatal(candidateErr)
		}
		candidates[index] = candidate
	}
	intent, err := agency.NewAgentIntent(agency.IntentSpec{Kind: kind,
		Consequence: agency.ConsequenceCreateHandlings, Successors: []agency.TargetRef{agency.SelfTarget()},
		Artifacts: inputs})
	if err != nil {
		t.Fatal(err)
	}
	request, err := agency.BindIntent(agency.BoundIntentSpec{Intent: intent,
		OperationKey: operation, View: view, Candidates: candidates})
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func seedReceiptFixture(t testing.TB, request agency.BoundIntent, accepted bool,
	name string,
) agency.Receipt {
	t.Helper()
	now := time.Date(2026, 8, 3, 12, 1, 0, 0, time.UTC)
	if !accepted {
		code, err := agency.NewSemanticLabel("selection.seed.rejected")
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := agency.NewRejectedReceipt(request, code, "not accepted", now)
		if err != nil {
			t.Fatal(err)
		}
		return receipt
	}
	eventID, err := agency.NewEventID("event-" + name)
	if err != nil {
		t.Fatal(err)
	}
	event, err := agency.NewEvent(request, agency.EventStamp{ID: eventID,
		AcceptedAt: now, OriginSequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := agency.NewAcceptedReceipt(request, event, now)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
