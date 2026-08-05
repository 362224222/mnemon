package main

import (
	"errors"
	"fmt"
)

func validateCombinedEvidence(proof evidence) error {
	global, err := indexGlobalEvents(proof.Nodes, true)
	if err != nil {
		return err
	}
	if err := validateGlobalCausation(global); err != nil {
		return err
	}
	if err := validateGlobalAuthorityRefs(global); err != nil {
		return err
	}
	if err := validateGlobalDeliveries(proof.Nodes, global); err != nil {
		return err
	}
	return validatePeerEffectSummary(proof)
}

func validateAuthoritySummary(nodes []nodeEvidence) error {
	global, err := indexGlobalEvents(nodes, false)
	if err != nil {
		return err
	}
	if err := validateGlobalCausation(global); err != nil {
		return err
	}
	if err := validateGlobalAuthorityRefs(global); err != nil {
		return err
	}
	return validateGlobalDeliveries(nodes, global)
}

func indexGlobalEvents(nodes []nodeEvidence, requireAcceptedReceipt bool) (map[string]eventEvidence, error) {
	global := make(map[string]eventEvidence)
	receiptCount, acceptedReceiptCount := 0, 0
	for _, node := range nodes {
		for _, event := range node.Events {
			if existing, duplicate := global[event.ID]; duplicate {
				return nil, fmt.Errorf("Event ID %q exists in both %s and %s",
					event.ID, existing.Node, node.Role)
			}
			global[event.ID] = event
		}
		for _, operation := range node.Operations {
			receiptCount++
			if operation.Outcome == "accepted" {
				acceptedReceiptCount++
			}
		}
	}
	if requireAcceptedReceipt && (receiptCount == 0 || acceptedReceiptCount == 0) {
		return nil, errors.New("authority snapshots contain no accepted operation Receipt")
	}
	return global, nil
}

func validateGlobalCausation(global map[string]eventEvidence) error {
	for _, event := range global {
		for _, causal := range event.Causation {
			known, exists := global[causal.ID]
			// Causal depth counts federation hops, not local transitions. A local
			// response therefore remains at the same depth as its accepted local
			// predecessor; only a regression below that depth is invalid.
			if !exists || known.Digest != causal.Digest || event.CausalDepth < known.CausalDepth {
				return fmt.Errorf("Event %q has invalid exact causal authority", event.ID)
			}
		}
	}
	return nil
}

func validateGlobalAuthorityRefs(global map[string]eventEvidence) error {
	for _, event := range global {
		for _, reference := range []*eventRefWire{event.SubjectHead,
			optionalEventRef(event.ReferenceHead, event.ReferenceDigest)} {
			if reference == nil {
				continue
			}
			known, exists := global[reference.ID]
			if !exists || known.Digest != reference.Digest {
				return fmt.Errorf("Event %q has a non-exact subject or Reference head", event.ID)
			}
		}
		if event.Correlation != nil {
			known, exists := global[event.Correlation.ID]
			if !exists || known.Digest != event.Correlation.Digest {
				return fmt.Errorf("Event %q has a mismatched correlation digest", event.ID)
			}
		}
	}
	return nil
}

func optionalEventRef(id, digest string) *eventRefWire {
	if id == "" && digest == "" {
		return nil
	}
	return &eventRefWire{ID: id, Digest: digest}
}

func validateGlobalDeliveries(nodes []nodeEvidence, global map[string]eventEvidence) error {
	for _, node := range nodes {
		for _, delivery := range node.Deliveries {
			origin, exists := global[delivery.OriginEventID]
			if !exists || origin.Digest != delivery.OriginEventDigest {
				return errors.New("peer Delivery has no exact collected origin Event")
			}
			if delivery.LocalEventID != "" {
				local, exists := global[delivery.LocalEventID]
				if !exists || local.Digest != delivery.LocalEventDigest {
					return errors.New("peer Receipt local Event differs from collected Event")
				}
			}
		}
	}
	return nil
}

func validatePeerEffectSummary(proof evidence) error {
	peerByRole := make(map[string]int, len(proof.Nodes))
	for _, node := range proof.Nodes {
		peerByRole[node.Role] = node.PeerEffects
	}
	reportByRole := make(map[string]int, len(proof.Report.Protocol.ByReceiver))
	for _, value := range proof.Report.Protocol.ByReceiver {
		reportByRole[value.Role] = value.AcceptedPeerEffects
	}
	if !mapsEqual(peerByRole, reportByRole) {
		return errors.New("stopped authority peer effects differ from sanitized live report")
	}
	return nil
}

func mapsEqual(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		other, exists := right[key]
		if !exists || other != value {
			return false
		}
	}
	return true
}
