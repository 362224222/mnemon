// Package semantic coordinates durable Peer Inbox snapshots with pure
// Teamwork policy. It performs no I/O and owns no durable state.
package semantic

import (
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

// PlanResult is exactly one terminal Store plan or one transient Teamwork
// diagnostic. A retry result cannot be passed to Store commit accidentally.
type PlanResult struct {
	plan            store.PeerInboxSemanticPlan
	retryDiagnostic string
	terminal        bool
	retry           bool
}

func (result PlanResult) Plan() (store.PeerInboxSemanticPlan, bool) {
	return result.plan, result.terminal
}

func (result PlanResult) RetryDiagnostic() (string, bool) {
	return result.retryDiagnostic, result.retry
}

type policySnapshot struct {
	local        model.PeerID
	imported     model.Event
	current      *model.ReviewWork
	causalEvents []model.Event
	decisionAt   time.Time
}

// PlanPeerInbox runs Teamwork policy against one immutable Store claim and
// freezes the terminal result into a Store-owned plan. Claim has already
// closed its SQLite transaction before it can reach this boundary.
func PlanPeerInbox(claim store.PeerInboxSemanticClaim, decisionAt time.Time) (PlanResult, error) {
	audience := claim.ImportedEvent().Audience().Peers()
	if len(audience) != 1 {
		return PlanResult{}, fmt.Errorf("plan Peer Inbox semantic: imported audience is not singular")
	}
	var current *model.ReviewWork
	if value, ok := claim.CurrentWork(); ok {
		copy := value
		current = &copy
	}
	policy, err := planPolicy(policySnapshot{local: audience[0], imported: claim.ImportedEvent(),
		current: current, causalEvents: claim.CausalEvents(), decisionAt: decisionAt})
	if err != nil {
		return PlanResult{}, err
	}
	if policy.Disposition() == teamwork.ImportRetry {
		return PlanResult{retry: true, retryDiagnostic: policy.Diagnostic()}, nil
	}
	spec, err := storePlanSpec(policy)
	if err != nil {
		return PlanResult{}, err
	}
	plan, err := store.NewPeerInboxSemanticPlan(claim, decisionAt, spec)
	if err != nil {
		return PlanResult{}, fmt.Errorf("plan Peer Inbox semantic: freeze Store plan: %w", err)
	}
	return PlanResult{plan: plan, terminal: true}, nil
}

func planPolicy(snapshot policySnapshot) (teamwork.ImportPlan, error) {
	facts := make([]teamwork.ImportEventFact, len(snapshot.causalEvents))
	for index, event := range snapshot.causalEvents {
		fact, err := teamwork.NewImportEventFact(event)
		if err != nil {
			return teamwork.ImportPlan{}, fmt.Errorf(
				"plan Peer Inbox semantic: causal Event %d: %w", index, err)
		}
		facts[index] = fact
	}
	plan, err := teamwork.PlanImportedEvent(teamwork.ImportPlanSpec{LocalPeerID: snapshot.local,
		Event: snapshot.imported, Current: snapshot.current, Facts: facts, Now: snapshot.decisionAt})
	if err != nil {
		return teamwork.ImportPlan{}, fmt.Errorf("plan Peer Inbox semantic: Teamwork policy: %w", err)
	}
	return plan, nil
}

func storePlanSpec(policy teamwork.ImportPlan) (store.PeerInboxSemanticPlanSpec, error) {
	disposition, err := storeDisposition(policy.Disposition())
	if err != nil {
		return store.PeerInboxSemanticPlanSpec{}, err
	}
	spec := store.PeerInboxSemanticPlanSpec{Disposition: disposition,
		Diagnostic: policy.Diagnostic()}
	if intent, exists := policy.Work(); exists {
		source, err := storeWorkSource(intent.Source())
		if err != nil {
			return store.PeerInboxSemanticPlanSpec{}, err
		}
		spec.Work = &store.PeerInboxSemanticWorkIntentSpec{Source: source,
			ResponseOrdinal: intent.ResponseOrdinal(), WorkRef: intent.WorkRef(),
			ChannelID: intent.ChannelID(), Participants: intent.Participants(),
			ExpectedVersion: intent.ExpectedVersion(), ExpectedState: intent.ExpectedState(),
			ExpectedIteration: intent.ExpectedIteration(), NextVersion: intent.NextVersion(),
			NextState: intent.NextState(), NextIteration: intent.NextIteration(),
			DeadlineUnixNano: intent.DeadlineUnixNano(), StateData: intent.StateData(),
			ObservedAtUnixNano: intent.ObservedAtUnixNano()}
	}
	if intent, exists := policy.Handling(); exists {
		source, err := storeHandlingSource(intent.Source())
		if err != nil {
			return store.PeerInboxSemanticPlanSpec{}, err
		}
		spec.Handling = &store.PeerInboxSemanticHandlingIntentSpec{Source: source,
			ResponseOrdinal: intent.ResponseOrdinal(), WorkRef: intent.WorkRef(),
			LocalRole: intent.LocalRole(), SourceEventType: intent.SourceEventType()}
	}
	if settlement, exists := policy.Settlement(); exists {
		disposition, err := storeSettlementDisposition(settlement.Disposition())
		if err != nil {
			return store.PeerInboxSemanticPlanSpec{}, err
		}
		spec.Settlement = &store.PeerInboxSemanticHandlingSettlementSpec{
			WorkRef: settlement.WorkRef(), SourceEventID: settlement.SourceEventID(),
			Disposition: disposition}
	}
	for _, response := range policy.Responses() {
		spec.Responses = append(spec.Responses, store.PeerInboxSemanticResponseIntentSpec{
			EventType: response.EventType(), Payload: response.Payload(), Cause: response.Cause()})
	}
	return spec, nil
}

func storeDisposition(value teamwork.ImportDisposition) (store.PeerInboxSemanticDisposition, error) {
	switch value {
	case teamwork.ImportApply:
		return store.PeerInboxSemanticApply, nil
	case teamwork.ImportReject:
		return store.PeerInboxSemanticReject, nil
	case teamwork.ImportConflict:
		return store.PeerInboxSemanticConflict, nil
	case teamwork.ImportReceiptOnly:
		return store.PeerInboxSemanticReceiptOnly, nil
	default:
		return "", fmt.Errorf("plan Peer Inbox semantic: nonterminal disposition %q", value)
	}
}

func storeWorkSource(source teamwork.ImportWorkSource) (store.PeerInboxSemanticEffectSource, error) {
	switch source {
	case teamwork.ImportWorkFromEvent:
		return store.PeerInboxSemanticFromImportedEvent, nil
	case teamwork.ImportWorkFromResponse:
		return store.PeerInboxSemanticFromLocalResponse, nil
	default:
		return "", fmt.Errorf("plan Peer Inbox semantic: Work source %q", source)
	}
}

func storeHandlingSource(source teamwork.ImportHandlingSource) (store.PeerInboxSemanticEffectSource, error) {
	switch source {
	case teamwork.ImportHandlingFromEvent:
		return store.PeerInboxSemanticFromImportedEvent, nil
	case teamwork.ImportHandlingFromResponse:
		return store.PeerInboxSemanticFromLocalResponse, nil
	default:
		return "", fmt.Errorf("plan Peer Inbox semantic: Handling source %q", source)
	}
}

func storeSettlementDisposition(value string) (store.PeerInboxSemanticSettlementDisposition, error) {
	switch value {
	case "superseded_cancelled":
		return store.PeerInboxSemanticSupersededCancelled, nil
	case "superseded_expired":
		return store.PeerInboxSemanticSupersededExpired, nil
	default:
		return "", fmt.Errorf("plan Peer Inbox semantic: settlement disposition %q", value)
	}
}
