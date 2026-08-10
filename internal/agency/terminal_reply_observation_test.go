package agency

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestTerminalPeerReplyBecomesZeroHandlingObservation(t *testing.T) {
	route := mustRoute(t, "route:terminal-observation")
	inReplyTo := mustDeliveryID(t, "delivery:original-request")
	delivery, err := NewPeerDelivery(route, PeerDeliverySpec{
		OriginEvent: mustEventRef(t, "event:remote-reply", "reply"), OriginSequence: 2,
		OriginAcceptedAt: testTime, OriginSource: mustPrincipal(t, "agent:remote"),
		OriginConsequence: ConsequenceResolveUnresolved, OriginTargetCount: 1,
		OriginCorrelation: mustEventRef(t, "event:request", "request"),
		InReplyToDelivery: inReplyTo, TargetAlias: mustHandle(t, "local/requester"),
		Kind: mustLabel(t, "review.response"), Payload: mustPayload(t, "Unable to conclude."),
		CausalDepth: 2, ExpiresAt: testTime.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(delivery.CanonicalJSON(), []byte(`"schema_version":3`)) ||
		!bytes.Contains(delivery.CanonicalJSON(), []byte(`"in_reply_to_delivery_id":"`+inReplyTo.String()+`"`)) {
		t.Fatalf("terminal PeerDelivery wire drift: %s", delivery.CanonicalJSON())
	}
	parsed, err := ParsePeerDeliveryCanonicalJSON(delivery.CanonicalJSON(), route)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := NewVerifiedPeerDelivery(parsed, mustPrincipal(t, "peer:remote"),
		mustPrincipal(t, "agent:local"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Consequence() != ConsequenceObserveUnresolved || verified.SuccessorCount() != 0 {
		t.Fatalf("verified reply effect = %s/%d", verified.Consequence(), verified.SuccessorCount())
	}
	event, err := NewPeerEvent(verified, EventStamp{ID: mustEventID(t, "event:local-observation"),
		AcceptedAt: testTime.Add(time.Minute), OriginSequence: 3, CausalDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	if event.Consequence() != ConsequenceObserveUnresolved || len(event.Targets()) != 0 {
		t.Fatalf("imported reply effect = %s targets=%d", event.Consequence(), len(event.Targets()))
	}
	if _, ok := event.Subject(); ok {
		t.Fatal("imported terminal observation unexpectedly owns a Handling subject")
	}
	if got, ok := event.InReplyToDelivery(); !ok || got != inReplyTo {
		t.Fatalf("imported observation in-reply-to = %v/%t", got, ok)
	}
}

func TestObservationConsequencesRemainMachineOnly(t *testing.T) {
	for _, consequence := range []Consequence{
		ConsequenceObserveCompleted, ConsequenceObserveDeclined, ConsequenceObserveUnresolved,
	} {
		if _, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "forged.observation"),
			Consequence: consequence}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Agent consequence %s error = %v, want ErrInvalid", consequence, err)
		}
		attachment := mustAttachment(t, "attachment:machine-only", mustPrincipal(t, "agent:local"), true)
		if _, err := NewViewAuthority(MachineViewSpec{Attachment: attachment,
			Consequences: []Consequence{consequence}}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("View consequence %s error = %v, want ErrInvalid", consequence, err)
		}
	}
}

func TestAgentViewProjectsTerminalReplyWithoutOpenHandlingCountCoupling(t *testing.T) {
	principal := mustPrincipal(t, "agent:view-observation")
	attachment := mustAttachment(t, "attachment:view-observation", principal, false)
	subjectHandle := mustHandle(t, "subject:view-observation")
	relatedHandle := mustHandle(t, "event:terminal-observation")
	authority := mustView(t, MachineViewSpec{
		Attachment: attachment, Consequences: []Consequence{ConsequenceAdvanceHandling},
		ReplyTo: subjectHandle,
		Subjects: []SubjectBinding{mustSubject(t, subjectHandle, "handling:view-observation",
			"event:view-head", "head", 2)},
		Provenance: []ProvenanceOffer{
			mustProvenance(t, subjectHandle, "event:view-head", "head"),
			mustProvenance(t, relatedHandle, "event:terminal-observation", "observation"),
		},
	})
	view, err := NewAgentView(AgentViewSpec{
		Handle: mustHandle(t, "view:terminal-observation"), Authority: authority,
		Current: &AgentViewCurrentSpec{Subject: subjectHandle, ReplyTo: subjectHandle,
			Kind: mustLabel(t, "work.current"), Payload: mustPayload(t, "Continue the request.")},
		Related: []AgentViewRelatedSpec{{Event: relatedHandle,
			Relation: AgentViewRelationTerminalReply, Outcome: AgentViewTerminalOutcomeCompleted,
			Kind: mustLabel(t, "review.response"), Payload: mustPayload(t, "Accepted.")}},
		Outstanding: AgentViewOutstanding{OpenTotal: 1, RelatedTotal: 2,
			RelatedProjected: 1, Truncated: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(view.CanonicalJSON(), []byte(`"related_open"`)) {
		t.Fatalf("v8 View retained related_open: %s", view.CanonicalJSON())
	}
	var wire agentViewWire
	if err := json.Unmarshal(view.CanonicalJSON(), &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Version != AgentViewVersion || len(wire.Related) != 1 ||
		wire.Related[0].Facts.Relation != "terminal_reply" ||
		wire.Related[0].Facts.Outcome != "completed" || wire.Outstanding.RelatedTotal != 2 {
		t.Fatalf("terminal reply projection drift: %#v", wire)
	}
	if _, err := ParseAgentViewCanonicalJSON(view.CanonicalJSON(), authority); err != nil {
		t.Fatalf("ParseAgentViewCanonicalJSON() error = %v", err)
	}
}
