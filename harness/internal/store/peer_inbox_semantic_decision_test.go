package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

func TestPeerInboxSemanticDecisionResponseEventIDIsStableAndOrdinalBound(t *testing.T) {
	seed := model.Sum([]byte("decision-response-seed"))
	want := []string{
		"event-semantic-e7b895ed3a4fdbf45ab5c961d822c6dc67e387ad511aab17a93e9bdba9dca66a",
		"event-semantic-1105d95d2009fa93527069785c173c68dbc4d7af57fc4730252fc59e88a75ee8",
	}
	for ordinal := uint8(0); ordinal < 2; ordinal++ {
		first, err := PeerInboxSemanticResponseEventID(seed, ordinal)
		if err != nil || first.String() != want[ordinal] {
			t.Fatalf("response ID ordinal %d = (%s,%v), want %s",
				ordinal, first, err, want[ordinal])
		}
		second, err := PeerInboxSemanticResponseEventID(seed, ordinal)
		if err != nil || second != first {
			t.Fatalf("response ID ordinal %d is not deterministic: (%s,%s,%v)",
				ordinal, first, second, err)
		}
	}
	zero, err := PeerInboxSemanticResponseEventID(model.Digest{}, 0)
	if !errors.Is(err, ErrPeerInboxSemanticInput) || !zero.IsZero() {
		t.Fatalf("zero seed response ID = (%s,%v)", zero, err)
	}
	outOfRange, err := PeerInboxSemanticResponseEventID(seed, 2)
	if !errors.Is(err, ErrPeerInboxSemanticInput) || !outOfRange.IsZero() {
		t.Fatalf("out-of-range response ID = (%s,%v)", outOfRange, err)
	}
}

func TestPeerInboxSemanticDecisionCanonicalRoundTripZeroOneAndTwoResponses(t *testing.T) {
	names := []string{"zero", "one", "two"}
	for responseCount := 0; responseCount <= 2; responseCount++ {
		t.Run(names[responseCount], func(t *testing.T) {
			want := peerInboxSemanticDecisionTestValue(responseCount)
			encoded, err := encodePeerInboxSemanticDecision(want)
			if err != nil || encoded.IsZero() {
				t.Fatalf("encode decision with %d responses = (%s,%v)", responseCount, encoded, err)
			}
			got, canonical, err := decodePeerInboxSemanticDecision(encoded.Bytes())
			if err != nil {
				t.Fatalf("decode decision with %d responses: %v", responseCount, err)
			}
			if !reflect.DeepEqual(got, want) || canonical.String() != encoded.String() {
				t.Fatalf("decision round trip with %d responses drifted\nwant: %#v\ngot:  %#v",
					responseCount, want, got)
			}
			reencoded, err := encodePeerInboxSemanticDecision(got)
			if err != nil || !bytes.Equal(reencoded.Bytes(), encoded.Bytes()) {
				t.Fatalf("decision re-encode with %d responses = (%s,%v), want %s",
					responseCount, reencoded, err, encoded)
			}
		})
	}
}

func TestPeerInboxSemanticDecisionDecoderRejectsNoncanonicalAndOpenShapes(t *testing.T) {
	valid := peerInboxSemanticDecisionTestValue(1)
	canonical := mustEncodePeerInboxSemanticDecisionTest(t, valid)

	var unknown map[string]any
	if err := json.Unmarshal(canonical.Bytes(), &unknown); err != nil {
		t.Fatal(err)
	}
	unknown["unknown"] = true
	unknownJSON, err := model.JSONFrom(unknown)
	if err != nil {
		t.Fatal(err)
	}

	domain := valid
	domain.Domain = "mnemon/r5/peer-inbox-semantic-decision/2"
	version := valid
	version.SchemaVersion = 2
	attempt := valid
	attempt.Attempt = 0
	nilCausal := valid
	nilCausal.CausalEvents = nil
	nilResponses := valid
	nilResponses.Responses = nil
	nilPlanResponses := valid
	nilPlanResponses.Plan.Responses = nil
	statusMismatch := peerInboxSemanticDecisionTestValue(1)
	statusMismatch.Status = string(model.InboxConflicted)
	acceptedDiagnostic := peerInboxSemanticDecisionTestValue(1)
	acceptedDiagnostic.Plan.Diagnostic = "forged_diagnostic"
	retryDisposition := peerInboxSemanticDecisionTestValue(1)
	retryDisposition.Plan.Disposition = string(teamwork.ImportRetry)
	wrongReceipt := peerInboxSemanticDecisionTestValue(1)
	wrongReceipt.ReceiptEventID = "event-response-other"
	responseCountMismatch := peerInboxSemanticDecisionTestValue(1)
	responseCountMismatch.Plan.Responses = append(responseCountMismatch.Plan.Responses,
		responseCountMismatch.Plan.Responses[0])
	responseSource := peerInboxSemanticDecisionTestValue(1)
	responseSource.Responses[0].Source = string(model.EventSourceImported)
	importedSource := peerInboxSemanticDecisionTestValue(1)
	importedSource.ImportedEvent.Source = string(model.EventSourceLocal)
	commitBeforeDecision := peerInboxSemanticDecisionTestValue(1)
	commitBeforeDecision.CommittedAt = storeTime(time.Date(2026, time.July, 19, 3, 4, 5, 5,
		time.UTC))

	domainKey := []byte(`"domain":"` + peerInboxSemanticDecisionDomain + `"`)
	duplicateDomain := bytes.Replace(canonical.Bytes(), domainKey,
		append(append([]byte(nil), domainKey...), append([]byte(","), domainKey...)...), 1)
	cases := []struct {
		name string
		raw  []byte
	}{
		{"unknown field", unknownJSON.Bytes()},
		{"leading whitespace", append([]byte{' '}, canonical.Bytes()...)},
		{"duplicate field", duplicateDomain},
		{"trailing value", append(canonical.Bytes(), []byte(`{}`)...)},
		{"wrong domain", mustEncodePeerInboxSemanticDecisionTest(t, domain).Bytes()},
		{"wrong version", mustEncodePeerInboxSemanticDecisionTest(t, version).Bytes()},
		{"zero attempt", mustEncodePeerInboxSemanticDecisionTest(t, attempt).Bytes()},
		{"nil causal events", mustEncodePeerInboxSemanticDecisionTest(t, nilCausal).Bytes()},
		{"nil responses", mustEncodePeerInboxSemanticDecisionTest(t, nilResponses).Bytes()},
		{"nil plan responses", mustEncodePeerInboxSemanticDecisionTest(t, nilPlanResponses).Bytes()},
		{"status mismatch", mustEncodePeerInboxSemanticDecisionTest(t, statusMismatch).Bytes()},
		{"accepted diagnostic", mustEncodePeerInboxSemanticDecisionTest(t, acceptedDiagnostic).Bytes()},
		{"retry disposition", mustEncodePeerInboxSemanticDecisionTest(t, retryDisposition).Bytes()},
		{"wrong receipt", mustEncodePeerInboxSemanticDecisionTest(t, wrongReceipt).Bytes()},
		{"response count mismatch", mustEncodePeerInboxSemanticDecisionTest(t, responseCountMismatch).Bytes()},
		{"response source", mustEncodePeerInboxSemanticDecisionTest(t, responseSource).Bytes()},
		{"imported source", mustEncodePeerInboxSemanticDecisionTest(t, importedSource).Bytes()},
		{"commit before decision", mustEncodePeerInboxSemanticDecisionTest(t, commitBeforeDecision).Bytes()},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			decoded, rebuilt, err := decodePeerInboxSemanticDecision(test.raw)
			if !errors.Is(err, ErrPeerInboxSemanticInvariant) || !rebuilt.IsZero() ||
				!reflect.DeepEqual(decoded, peerInboxSemanticDecision{}) {
				t.Fatalf("decode malformed decision = (%#v,%s,%v)", decoded, rebuilt, err)
			}
		})
	}
}

func TestPeerInboxSemanticDecisionEnforcesExact64KiBBoundary(t *testing.T) {
	atLimit, atLimitJSON := peerInboxSemanticDecisionTestSized(t, 65536)
	encoded, err := encodePeerInboxSemanticDecision(atLimit)
	if err != nil || len(encoded.Bytes()) != 65536 || encoded.String() != atLimitJSON.String() {
		t.Fatalf("64 KiB decision encode = (%d,%v)", len(encoded.Bytes()), err)
	}
	if _, _, err := decodePeerInboxSemanticDecision(atLimitJSON.Bytes()); err != nil {
		t.Fatalf("64 KiB decision decode: %v", err)
	}

	overLimit, overLimitJSON := peerInboxSemanticDecisionTestSized(t, 65537)
	if _, err := encodePeerInboxSemanticDecision(overLimit); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
		t.Fatalf("64 KiB + 1 decision encode error = %v", err)
	}
	decoded, rebuilt, err := decodePeerInboxSemanticDecision(overLimitJSON.Bytes())
	if !errors.Is(err, ErrPeerInboxSemanticInvariant) || !rebuilt.IsZero() ||
		!reflect.DeepEqual(decoded, peerInboxSemanticDecision{}) {
		t.Fatalf("64 KiB + 1 decision decode = (%#v,%s,%v)", decoded, rebuilt, err)
	}
}

func TestPeerInboxSemanticDecisionCommitRequestDigestBindsEveryAuthorityDimension(t *testing.T) {
	fixture := newPeerInboxFixture(t, "semantic-decision-request-digest", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	audience, err := model.NewAudience([]model.PeerID{fixture.remote.Identity().PeerID()})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := fixture.store.PrepareLocalAdmission(context.Background(),
		fixture.channel.Channel().ID(), audience, 2)
	if err != nil {
		t.Fatal(err)
	}
	responses := []model.SignedPublication{
		fixture.publication(t, 41, 41, "semantic-decision-wire-a", true),
		fixture.publication(t, 42, 42, "semantic-decision-wire-b", true),
	}
	replacement := fixture.publication(t, 43, 43, "semantic-decision-wire-c", true)
	decisionAt := fixture.at.Add(10 * time.Second)
	fence := peerInboxSemanticRequestDigestFence(t, decisionAt)
	plan := peerInboxSemanticRequestDigestPlan(fence, decisionAt)

	baseline, err := peerInboxSemanticCommitRequestDigest(fence, plan, scope, responses)
	if err != nil || baseline.IsZero() {
		t.Fatalf("baseline request digest = (%s,%v)", baseline, err)
	}
	repeated, err := peerInboxSemanticCommitRequestDigest(fence, plan, scope, responses)
	if err != nil || repeated != baseline {
		t.Fatalf("request digest is not deterministic = (%s,%s,%v)", baseline, repeated, err)
	}

	type requestVariant struct {
		name      string
		fence     PeerInboxSemanticFence
		plan      PeerInboxSemanticPlan
		scope     LocalAdmissionScope
		responses []model.SignedPublication
	}
	variants := make([]requestVariant, 0, 32)
	addFence := func(name string, mutate func(*PeerInboxSemanticFence)) {
		candidate := fence
		mutate(&candidate)
		variants = append(variants, requestVariant{name: "fence/" + name, fence: candidate,
			plan: plan, scope: scope, responses: responses})
	}
	otherInbox, _ := model.ParseInboxID("inbox-semantic-decision-request-other")
	addFence("inbox", func(value *PeerInboxSemanticFence) { value.inboxID = otherInbox })
	addFence("owner", func(value *PeerInboxSemanticFence) { value.leaseOwner += "-other" })
	addFence("lease", func(value *PeerInboxSemanticFence) { value.leaseUntil = value.leaseUntil.Add(time.Nanosecond) })
	addFence("attempt", func(value *PeerInboxSemanticFence) { value.attempt++ })
	addFence("nonce", func(value *PeerInboxSemanticFence) { value.semanticNonce[0] ^= 0xff })
	addFence("snapshot", func(value *PeerInboxSemanticFence) {
		value.snapshotDigest = model.Sum([]byte("semantic-decision-snapshot-other"))
	})

	addScope := func(name string, candidate LocalAdmissionScope) {
		variants = append(variants, requestVariant{name: "scope/" + name, fence: fence,
			plan: plan, scope: candidate, responses: responses})
	}
	channel, _ := model.ParseChannelID("channel-semantic-decision-other")
	candidate := scope
	candidate.channelID = channel
	addScope("channel", candidate)
	candidate = scope
	candidate.firstOriginSequence++
	addScope("first origin sequence", candidate)
	candidate = scope
	candidate.firstChannelSequence++
	addScope("first channel sequence", candidate)
	candidate = scope
	candidate.originMember = peerInboxSemanticDecisionTestRecordHead(t,
		scope.originMember.Revision()+1, scope.originMember.Digest())
	addScope("origin member revision", candidate)
	candidate = scope
	candidate.originMember = peerInboxSemanticDecisionTestRecordHead(t,
		scope.originMember.Revision(), model.Sum([]byte("semantic-origin-member-other")))
	addScope("origin member digest", candidate)
	candidate = scope
	candidate.publicationRoster = peerInboxSemanticDecisionTestRecordHead(t,
		scope.publicationRoster.Revision()+1, scope.publicationRoster.Digest())
	addScope("roster revision", candidate)
	candidate = scope
	candidate.publicationRoster = peerInboxSemanticDecisionTestRecordHead(t,
		scope.publicationRoster.Revision(), model.Sum([]byte("semantic-roster-other")))
	addScope("roster digest", candidate)

	addNodeScope := func(name string, mutate func(*model.NodeSpec)) {
		candidate := scope
		spec := candidate.node.Spec()
		mutate(&spec)
		node, err := model.NewNode(spec)
		if err != nil {
			t.Fatalf("build Node scope variant %q: %v", name, err)
		}
		candidate.node = node
		addScope("node "+name, candidate)
	}
	peer, _ := model.ParsePeerID("peer-semantic-decision-other")
	addNodeScope("peer", func(value *model.NodeSpec) { value.PeerID = peer })
	epoch, _ := model.ParseOriginEpoch("epoch-semantic-decision-other")
	addNodeScope("epoch", func(value *model.NodeSpec) { value.OriginEpoch = epoch })
	addNodeScope("asset revision", func(value *model.NodeSpec) { value.ActiveAssetRevision += "-other" })
	addNodeScope("created at", func(value *model.NodeSpec) { value.CreatedAt = value.CreatedAt.Add(-time.Nanosecond) })
	addNodeScope("updated at", func(value *model.NodeSpec) { value.UpdatedAt = value.UpdatedAt.Add(time.Nanosecond) })

	addProfileScope := func(name string, mutate func(*model.ProfileSpec)) {
		candidate := scope
		spec := candidate.profile.Spec()
		mutate(&spec)
		profile, err := model.NewProfile(spec)
		if err != nil {
			t.Fatalf("build Profile scope variant %q: %v", name, err)
		}
		candidate.profile = profile
		addScope("profile "+name, candidate)
	}
	addProfileScope("principal", func(value *model.ProfileSpec) { value.Principal += "-other" })
	addProfileScope("workspace", func(value *model.ProfileSpec) { value.WorkspaceRoot += "-other" })
	addProfileScope("host runtime", func(value *model.ProfileSpec) {
		value.Host, value.Runtime = model.HostClaudeCode, model.RuntimeClaudeCLI
	})
	addProfileScope("credential", func(value *model.ProfileSpec) {
		value.CredentialHash = model.Sum([]byte("semantic-profile-credential-other"))
	})
	addProfileScope("asset revision", func(value *model.ProfileSpec) { value.ActiveAssetRevision += "-other" })
	addProfileScope("handling budget", func(value *model.ProfileSpec) {
		budgetSpec := model.DefaultHandlingBudget().Spec()
		budgetSpec.MaxAttempts++
		budget, err := model.NewHandlingBudget(budgetSpec)
		if err != nil {
			t.Fatal(err)
		}
		value.HandlingBudget = budget.JSON()
	})
	addProfileScope("enabled", func(value *model.ProfileSpec) { value.Enabled = !value.Enabled })
	addProfileScope("created at", func(value *model.ProfileSpec) { value.CreatedAt = value.CreatedAt.Add(-time.Nanosecond) })
	addProfileScope("updated at", func(value *model.ProfileSpec) { value.UpdatedAt = value.UpdatedAt.Add(time.Nanosecond) })

	candidate = scope
	candidate.count--
	variants = append(variants, requestVariant{name: "scope/count", fence: fence,
		plan: plan, scope: candidate, responses: responses})
	variants = append(variants,
		requestVariant{name: "wire", fence: fence, plan: plan, scope: scope,
			responses: []model.SignedPublication{responses[0], replacement}},
		requestVariant{name: "response count", fence: fence, plan: plan,
			scope: scope, responses: responses[:1]},
		requestVariant{name: "order", fence: fence, plan: plan, scope: scope,
			responses: []model.SignedPublication{responses[1], responses[0]}})

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			got, err := peerInboxSemanticCommitRequestDigest(variant.fence, variant.plan,
				variant.scope, variant.responses)
			if err != nil {
				t.Fatalf("variant request digest: %v", err)
			}
			if got == baseline {
				t.Fatalf("variant did not change request digest %s", baseline)
			}
		})
	}

	if _, err := peerInboxSemanticCommitRequestDigest(fence, plan, scope,
		[]model.SignedPublication{{}}); !errors.Is(err, ErrPeerInboxSemanticInput) {
		t.Fatalf("incomplete response publication error = %v", err)
	}
}

func peerInboxSemanticDecisionTestValue(responseCount int) peerInboxSemanticDecision {
	decidedAt := time.Date(2026, time.July, 19, 3, 4, 5, 6, time.UTC)
	responses := make([]peerInboxSemanticEventDecision, responseCount)
	intents := make([]peerInboxSemanticResponseIntentDecision, responseCount)
	for index := range responses {
		label := string(rune('a' + index))
		responses[index] = peerInboxSemanticEventDecision{
			EventDigest: model.Sum([]byte("response-event-" + label)).String(),
			EventID:     "event-response-" + label, OriginEpoch: "epoch-local",
			OriginPeerID:      "peer-local",
			PublicationDigest: model.Sum([]byte("response-publication-" + label)).String(),
			Source:            string(model.EventSourceLocal),
		}
		intents[index] = peerInboxSemanticResponseIntentDecision{
			CauseEventID: "event-imported", CauseOriginEpoch: "epoch-remote",
			CauseOriginPeerID: "peer-remote", EventType: string(model.EventReviewAccepted),
			Payload: `{"work_version":2}`,
		}
	}
	receipt := ""
	if responseCount != 0 {
		receipt = responses[responseCount-1].EventID
	}
	return peerInboxSemanticDecision{
		Attempt: 3,
		CausalEvents: []peerInboxSemanticEventDecision{{
			EventDigest: model.Sum([]byte("causal-event")).String(), EventID: "event-causal",
			OriginEpoch: "epoch-local", OriginPeerID: "peer-local",
			PublicationDigest: model.Sum([]byte("causal-publication")).String(),
			Source:            string(model.EventSourceLocal),
		}},
		CommittedAt: storeTime(decidedAt.Add(time.Second)), DecidedAt: storeTime(decidedAt),
		DecisionSeed: model.Sum([]byte("decision-seed")).String(), Domain: peerInboxSemanticDecisionDomain,
		ImportedEvent: peerInboxSemanticEventDecision{
			EventDigest: model.Sum([]byte("imported-event")).String(), EventID: "event-imported",
			OriginEpoch: "epoch-remote", OriginPeerID: "peer-remote",
			PublicationDigest: model.Sum([]byte("imported-publication")).String(),
			Source:            string(model.EventSourceImported),
		},
		InboxID: "inbox-semantic-decision", Plan: peerInboxSemanticPlanDecision{
			Diagnostic: "", Disposition: "apply", InboxStatus: string(model.InboxAccepted),
			Responses: intents,
		},
		PriorWork: nil, ReceiptEventID: receipt,
		RequestDigest: model.Sum([]byte("request-digest")).String(), Responses: responses,
		SchemaVersion: 1, SnapshotDigest: model.Sum([]byte("snapshot-digest")).String(),
		Status: string(model.InboxAccepted),
	}
}

func mustEncodePeerInboxSemanticDecisionTest(t *testing.T,
	value peerInboxSemanticDecision,
) model.JSON {
	t.Helper()
	encoded, err := encodePeerInboxSemanticDecision(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func peerInboxSemanticDecisionTestSized(t *testing.T, target int) (peerInboxSemanticDecision, model.JSON) {
	t.Helper()
	value := peerInboxSemanticDecisionTestValue(0)
	value.Plan.Disposition = string(teamwork.ImportConflict)
	value.Plan.InboxStatus = string(model.InboxConflicted)
	value.Plan.Diagnostic = "x"
	value.Status = string(model.InboxConflicted)
	value.PriorWork = &peerInboxSemanticWorkDecision{ChannelID: "channel-sized-decision",
		DeadlineUnixNano: time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC).UnixNano(),
		HomePeerID:       "peer-local", InitiatorPeerID: "peer-local", Iteration: 1,
		ReviewerPeerID: "peer-remote", RosterRevision: 1, State: string(model.WorkOffered),
		StateData: `""`, UpdatedAt: value.DecidedAt, UpdatedByEvent: "event-causal",
		Version: 1, WorkID: "work-sized-decision"}
	base, err := model.JSONFrom(value)
	if err != nil {
		t.Fatal(err)
	}
	padding := target - len(base.Bytes())
	if padding < 0 {
		t.Fatalf("decision fixture %d exceeds target %d", len(base.Bytes()), target)
	}
	value.PriorWork.StateData = `"` + strings.Repeat("x", padding) + `"`
	canonical, err := model.JSONFrom(value)
	if err != nil {
		t.Fatal(err)
	}
	if len(canonical.Bytes()) != target {
		t.Fatalf("sized decision length = %d, want %d", len(canonical.Bytes()), target)
	}
	return value, canonical
}

func peerInboxSemanticDecisionTestRecordHead(t *testing.T, revision uint64,
	digest model.Digest,
) model.RecordHead {
	t.Helper()
	head, err := model.NewRecordHead(revision, digest)
	if err != nil {
		t.Fatal(err)
	}
	return head
}
