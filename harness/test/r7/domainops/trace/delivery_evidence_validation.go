package main

import (
	"errors"
	"slices"
)

func validateGlobalDeliveries(nodes []nodeEvidence, global map[string]eventEvidence) error {
	for _, node := range nodes {
		if err := validateNodeDeliveries(node, global); err != nil {
			return err
		}
	}
	return nil
}

type nodeDeliveryValidation struct {
	events          map[string]eventEvidence
	outbox          map[string]deliveryEvidence
	handlings       map[string]handlingEvidence
	acceptedReplies map[string]struct{}
}

func validateNodeDeliveries(node nodeEvidence, global map[string]eventEvidence) error {
	validation := indexNodeDeliveryValidation(node)
	for _, delivery := range node.Deliveries {
		if err := validateCollectedDelivery(delivery, global, &validation); err != nil {
			return err
		}
	}
	return nil
}

func indexNodeDeliveryValidation(node nodeEvidence) nodeDeliveryValidation {
	validation := nodeDeliveryValidation{
		events:          make(map[string]eventEvidence, len(node.Events)),
		outbox:          make(map[string]deliveryEvidence),
		handlings:       make(map[string]handlingEvidence, len(node.Handlings)),
		acceptedReplies: make(map[string]struct{}),
	}
	for _, event := range node.Events {
		validation.events[event.ID] = event
	}
	for _, handling := range node.Handlings {
		validation.handlings[handling.ID] = handling
	}
	for _, delivery := range node.Deliveries {
		if delivery.Direction == "outbox" {
			validation.outbox[delivery.ID] = delivery
		}
	}
	return validation
}

func validateCollectedDelivery(delivery deliveryEvidence, global map[string]eventEvidence,
	validation *nodeDeliveryValidation,
) error {
	origin, exists := global[delivery.OriginEventID]
	if !exists || origin.Digest != delivery.OriginEventDigest {
		return errors.New("peer Delivery has no exact collected origin Event")
	}
	if delivery.EnvelopeDigest != "" {
		if err := validateDeliveryOrigin(origin, delivery); err != nil {
			return err
		}
	}
	return validateDeliveryLocalEffect(delivery, global, validation)
}

func validateDeliveryLocalEffect(delivery deliveryEvidence, global map[string]eventEvidence,
	validation *nodeDeliveryValidation,
) error {
	if delivery.LocalEventID == "" {
		if delivery.Accepted {
			return errors.New("accepted inbox Delivery has no exact local Event")
		}
		return nil
	}
	local, exists := global[delivery.LocalEventID]
	if !exists || local.Digest != delivery.LocalEventDigest {
		return errors.New("peer Receipt local Event differs from collected Event")
	}
	if delivery.Direction != "inbox" || !delivery.Accepted {
		return errors.New("only an accepted inbox Delivery may name a local Event")
	}
	return validateReadmittedEvent(local, delivery, validation)
}

func validateDeliveryOrigin(origin eventEvidence, delivery deliveryEvidence) error {
	if origin.OriginSequence != delivery.OriginSequence ||
		!origin.AcceptedAt.Equal(delivery.OriginAcceptedAt) ||
		origin.SourcePrincipal != delivery.OriginSource ||
		origin.Consequence != delivery.OriginConsequence ||
		len(origin.Targets) != delivery.OriginTargetCount ||
		int(origin.CausalDepth)+1 != int(delivery.OriginCausalDepth) ||
		origin.SemanticKind != delivery.OriginSemanticKind ||
		origin.PayloadBytes != delivery.OriginPayloadBytes ||
		!slices.Equal(origin.Artifacts, delivery.OriginArtifacts) ||
		!eventRefsEqual(origin.Causation, delivery.OriginCausation) ||
		!optionalEventRefsEqual(origin.Correlation, delivery.OriginCorrelation) {
		return errors.New("peer Delivery signed origin differs from collected Event authority")
	}
	return nil
}

func validateReadmittedEvent(local eventEvidence, delivery deliveryEvidence,
	validation *nodeDeliveryValidation,
) error {
	if local.RequestDigest != delivery.EnvelopeDigest ||
		local.CausalDepth != delivery.OriginCausalDepth ||
		local.SemanticKind != delivery.OriginSemanticKind ||
		local.PayloadBytes != delivery.OriginPayloadBytes ||
		!slices.Equal(local.Artifacts, delivery.OriginArtifacts) ||
		len(local.Causation) != 1 || local.Causation[0].ID != delivery.OriginEventID ||
		local.Causation[0].Digest != delivery.OriginEventDigest ||
		!optionalEventRefsEqual(local.Correlation, delivery.OriginCorrelation) {
		return errors.New("readmitted local Event differs from signed peer candidate")
	}
	if delivery.InReplyToDeliveryID == "" {
		return validateOrdinaryReadmittedEvent(local)
	}
	return validateTerminalReplyObservation(local, delivery, validation)
}

func validateOrdinaryReadmittedEvent(local eventEvidence) error {
	if local.InReplyToDelivery != "" || local.Consequence != "handling.create" ||
		len(local.Targets) != 1 || local.SubjectHandling != "" || local.ReferenceKey != "" {
		return errors.New("ordinary peer Delivery did not create exactly one local Handling Event")
	}
	return nil
}

func validateTerminalReplyObservation(local eventEvidence, delivery deliveryEvidence,
	validation *nodeDeliveryValidation,
) error {
	if _, duplicate := validation.acceptedReplies[delivery.InReplyToDeliveryID]; duplicate {
		return errors.New("more than one accepted terminal observation names one outbound Delivery")
	}
	validation.acceptedReplies[delivery.InReplyToDeliveryID] = struct{}{}
	request, exists := validation.outbox[delivery.InReplyToDeliveryID]
	if !exists || request.RouteID != delivery.RouteID || request.ReplyAnchorHandlingID == "" ||
		request.ExpectedReplyRootID == "" || request.ExpectedReplyRootDigest == "" {
		return errors.New("terminal observation has no exact local outbound reply binding")
	}
	if err := validateTerminalObservationAnchor(local, request, validation); err != nil {
		return err
	}
	if delivery.OriginCorrelation == nil ||
		delivery.OriginCorrelation.ID != request.ExpectedReplyRootID ||
		delivery.OriginCorrelation.Digest != request.ExpectedReplyRootDigest {
		return errors.New("terminal observation correlation differs from outbound reply root")
	}
	return validateTerminalObservationShape(local, delivery)
}

func validateTerminalObservationAnchor(local eventEvidence, request deliveryEvidence,
	validation *nodeDeliveryValidation,
) error {
	anchor, exists := validation.handlings[request.ReplyAnchorHandlingID]
	if !exists || anchor.CreatedSequence >= local.OriginSequence || anchor.HeadEventID == local.ID {
		return errors.New("terminal observation has no older unchanged local reply anchor")
	}
	head, exists := validation.events[anchor.HeadEventID]
	if !exists || head.Node != local.Node {
		return errors.New("terminal observation reply anchor head is not same-node Event authority")
	}
	switch anchor.State {
	case "open":
		return nil
	case "terminal":
		// A stopped snapshot may contain a later explicit local decision. It
		// cannot prove the earlier peer observation was admitted against an
		// open anchor unless that terminal head is strictly later.
	default:
		return errors.New("terminal observation reply anchor has an invalid durable state")
	}
	if head.SubjectHandling != anchor.ID || head.OriginSequence <= local.OriginSequence ||
		!isTerminalHandlingConsequence(head.Consequence) {
		return errors.New("terminal observation reply anchor was not closed by a later exact local Event")
	}
	return nil
}

func isTerminalHandlingConsequence(consequence string) bool {
	switch consequence {
	case "handling.resolve.completed", "handling.resolve.declined", "handling.resolve.unresolved":
		return true
	default:
		return false
	}
}

func validateTerminalObservationShape(local eventEvidence, delivery deliveryEvidence) error {
	wantConsequence, ok := observationConsequence(delivery.OriginConsequence)
	if !ok || local.Consequence != wantConsequence || local.InReplyToDelivery != delivery.InReplyToDeliveryID ||
		len(local.Targets) != 0 || local.SubjectHandling != "" || local.ReferenceKey != "" {
		return errors.New("terminal reply did not produce the exact zero-target observation Event")
	}
	return nil
}

func observationConsequence(origin string) (string, bool) {
	switch origin {
	case "handling.resolve.completed":
		return "observation.completed", true
	case "handling.resolve.declined":
		return "observation.declined", true
	case "handling.resolve.unresolved":
		return "observation.unresolved", true
	default:
		return "", false
	}
}

func eventRefsEqual(left, right []eventRefWire) bool {
	return slices.EqualFunc(left, right, func(a, b eventRefWire) bool {
		return a == b
	})
}

func optionalEventRefsEqual(left, right *eventRefWire) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
