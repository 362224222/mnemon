package agency

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestExactTerminalReplyBindsDeliveryAndObservationReadSet(t *testing.T) {
	principal := mustPrincipal(t, "agent:replying")
	attachment := mustAttachment(t, "attachment:replying", principal, false)
	subjectHandle := mustHandle(t, "subject:replying")
	subject, err := NewSubjectBinding(subjectHandle, mustHandlingID(t, "handling:replying"),
		mustEventRef(t, "event:reply-head", "head"), 4, 7)
	if err != nil {
		t.Fatal(err)
	}
	replyHandle := mustHandle(t, "event:request")
	targetRef := mustAliasTarget(t, "target:requester")
	target, _ := ResolveRemoteTarget(targetRef, mustRoute(t, "route:requester"),
		mustHandle(t, "peer:requester"))
	replyDelivery := mustDeliveryID(t, "delivery:remote-request")
	view := mustView(t, MachineViewSpec{
		Attachment: attachment, Consequences: []Consequence{ConsequenceResolveDeclined},
		Subjects: []SubjectBinding{subject}, Targets: []ResolvedTarget{target},
		ReplyTo: replyHandle, ReplyTarget: targetRef, ReplyDelivery: replyDelivery,
		Provenance: []ProvenanceOffer{mustProvenance(t, replyHandle,
			"event:remote-request", "request")},
	})
	intent, err := NewAgentIntent(IntentSpec{
		Kind: mustLabel(t, "review.response"), Payload: mustPayload(t, "Declined with reasons."),
		Consequence: ConsequenceResolveDeclined, SubjectHandling: subjectHandle,
		Successors: []TargetRef{targetRef}, CorrelationHandle: replyHandle,
	})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindIntent(BoundIntentSpec{Intent: intent,
		OperationKey: mustOperation(t, "operation:terminal-reply"), View: view})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := bound.InReplyToDelivery(); !ok || got != replyDelivery {
		t.Fatalf("BoundIntent in-reply-to = %v/%t, want %v/true", got, ok, replyDelivery)
	}
	var boundWire boundIntentWire
	if err := json.Unmarshal(bound.CanonicalJSON(), &boundWire); err != nil {
		t.Fatal(err)
	}
	if boundWire.SchemaVersion != 3 || boundWire.Request.SchemaVersion != 3 ||
		boundWire.Request.InReplyToDelivery != replyDelivery.String() ||
		boundWire.Request.Subject == nil || boundWire.Request.Subject.ObservationRevision != 7 {
		t.Fatalf("bound terminal reply lost exact machine authority: %#v", boundWire)
	}
	event, err := NewEvent(bound, EventStamp{ID: mustEventID(t, "event:terminal-reply"),
		AcceptedAt: testTime, OriginSequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := event.InReplyToDelivery(); !ok || got != replyDelivery {
		t.Fatalf("Event in-reply-to = %v/%t, want %v/true", got, ok, replyDelivery)
	}
	var eventWire eventWire
	if err := json.Unmarshal(event.CanonicalJSON(), &eventWire); err != nil {
		t.Fatal(err)
	}
	if eventWire.SchemaVersion != 3 || eventWire.Machine.InReplyToDelivery != replyDelivery.String() ||
		eventWire.Machine.Subject == nil || eventWire.Machine.Subject.ObservationRevision != 7 {
		t.Fatalf("Event terminal reply lost exact machine authority: %#v", eventWire)
	}
}

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
		t.Fatalf("v6 View retained related_open: %s", view.CanonicalJSON())
	}
	var wire agentViewWire
	if err := json.Unmarshal(view.CanonicalJSON(), &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Version != 6 || len(wire.Related) != 1 ||
		wire.Related[0].Facts.Relation != "terminal_reply" ||
		wire.Related[0].Facts.Outcome != "completed" || wire.Outstanding.RelatedTotal != 2 {
		t.Fatalf("terminal reply projection drift: %#v", wire)
	}
	if _, err := ParseAgentViewCanonicalJSON(view.CanonicalJSON(), authority); err != nil {
		t.Fatalf("ParseAgentViewCanonicalJSON() error = %v", err)
	}
}
