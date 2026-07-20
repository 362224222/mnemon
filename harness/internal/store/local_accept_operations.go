package store

import (
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func validateOperationEvents(authority *LocalOperationAuthority, events []model.Event,
	semanticControllerBatch bool,
) error {
	if authority == nil {
		return validateControllerEvents(events, semanticControllerBatch)
	}
	return validateOperationKindEvents(authority.Kind, events)
}

// validateControllerEvents admits exactly one controller-authoritative Event
// with source causality, or the closed semantic deadline batch.
func validateControllerEvents(events []model.Event, semanticControllerBatch bool) error {
	if semanticControllerBatch {
		return validateSemanticControllerBatch(events)
	}
	if len(events) != 1 {
		return errors.New("commit local acceptance: controller accepts exactly one Event")
	}
	switch events[0].Type() {
	case model.EventReviewAccepted, model.EventReviewAcceptRejected, model.EventReviewDelivered,
		model.EventReviewDeclined, model.EventReviewExpired, model.EventReviewOutcome:
		if len(events[0].CausedBy()) == 0 {
			return errors.New("commit local acceptance: controller Event requires source causality")
		}
		return nil
	default:
		return errors.New("commit local acceptance: Event type is not controller-authoritative")
	}
}

// validateSemanticControllerBatch admits the one closed two-Event deadline
// decision: review.expired followed by its request receipt, each carrying
// exactly one source cause.
func validateSemanticControllerBatch(events []model.Event) error {
	if len(events) != 2 || events[0].Type() != model.EventReviewExpired ||
		(events[1].Type() != model.EventReviewAcceptRejected &&
			events[1].Type() != model.EventReviewOutcome) ||
		len(events[0].CausedBy()) != 1 || len(events[1].CausedBy()) != 1 {
		return errors.New("commit local acceptance: invalid semantic deadline controller batch")
	}
	return nil
}

// validateOperationKindEvents requires every Event of an operation-authorized
// batch to carry the single Event type its operation kind admits.
func validateOperationKindEvents(kind model.OperationKind, events []model.Event) error {
	want := map[model.OperationKind]model.EventType{
		model.OperationTeamworkOffer: model.EventReviewOffered, model.OperationTeamworkAccept: model.EventReviewAcceptRequested,
		model.OperationTeamworkDecline: model.EventReviewDeclineRequested, model.OperationTeamworkDeliver: model.EventReviewDeliveryReady,
		model.OperationTeamworkRework: model.EventReviewReworkRequested, model.OperationTeamworkClose: model.EventReviewClosed,
		model.OperationTeamworkCancel: model.EventReviewCancelled,
	}[kind]
	if !want.Valid() {
		return errors.New("commit local acceptance: operation kind does not admit an Event")
	}
	if len(events) > 1 && want != model.EventReviewOffered {
		return errors.New("commit local acceptance: only teamwork.offer may expand a batch")
	}
	var previousReviewer model.PeerID
	for index, event := range events {
		if event.Type() != want {
			return fmt.Errorf("commit local acceptance: operation %s cannot emit %s", kind, event.Type())
		}
		if want == model.EventReviewOffered {
			reviewer, err := validateExpandedOfferEvent(events, index, previousReviewer)
			if err != nil {
				return err
			}
			previousReviewer = reviewer
		} else if len(event.CausedBy()) == 0 {
			return errors.New("commit local acceptance: context action requires source causality")
		}
	}
	return nil
}

// validateExpandedOfferEvent requires canonical unique reviewer order and
// identical offer semantics across every expanded review.offered Event.
func validateExpandedOfferEvent(events []model.Event, index int,
	previousReviewer model.PeerID,
) (model.PeerID, error) {
	event := events[index]
	if event.Audience().Len() != 1 {
		return model.PeerID{}, errors.New("commit local acceptance: offer batch must use canonical unique reviewer order")
	}
	reviewer := event.Audience().Peers()[0]
	if index > 0 {
		comparison, err := model.ComparePeerIDs(previousReviewer, reviewer)
		if err != nil || comparison >= 0 {
			return model.PeerID{}, errors.New("commit local acceptance: offer batch must use canonical unique reviewer order")
		}
	}
	if index > 0 && !sameExpandedOfferSemantics(events[0], event) {
		return model.PeerID{}, errors.New("commit local acceptance: expanded offers changed content, deadline, Artifact or causality")
	}
	return reviewer, nil
}

func sameExpandedOfferSemantics(left, right model.Event) bool {
	leftArtifacts, _ := model.JSONFrom(left.Artifacts())
	rightArtifacts, _ := model.JSONFrom(right.Artifacts())
	leftCauses, _ := model.JSONFrom(left.CausedBy())
	rightCauses, _ := model.JSONFrom(right.CausedBy())
	return left.Summary() == right.Summary() && left.Payload().String() == right.Payload().String() &&
		leftArtifacts.String() == rightArtifacts.String() && leftCauses.String() == rightCauses.String()
}
