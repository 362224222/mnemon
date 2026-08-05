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
	if err := validatePeerEffectSummary(proof); err != nil {
		return err
	}
	return validateEvolutionEvidence(proof)
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

func validateEvolutionEvidence(proof evidence) error {
	nodes := make(map[string]nodeEvidence, len(proof.Nodes))
	for _, node := range proof.Nodes {
		nodes[node.Role] = node
	}
	boundary, err := validateEvolutionBoundary(nodes,
		proof.Report.Protocol.Evolution.Boundary.Nodes)
	if err != nil {
		return err
	}
	total, err := validateEvolutionEffects(nodes, boundary,
		proof.Report.Protocol.Evolution.Effects)
	if err != nil {
		return err
	}
	if total != proof.Report.Protocol.Evolution.AcceptedReferenceUses || total < 1 {
		return errors.New("stopped authority does not prove a later Reference use")
	}
	return nil
}

func validateEvolutionBoundary(nodes map[string]nodeEvidence,
	values []evolutionBoundaryNode,
) (map[string]evolutionBoundaryNode, error) {
	boundary := make(map[string]evolutionBoundaryNode, len(domainRoles))
	for _, value := range values {
		node, exists := nodes[value.Role]
		if !exists {
			return nil, errors.New("evolution boundary names an absent authority node")
		}
		if err := validateEvolutionBoundaryNode(node, value); err != nil {
			return nil, err
		}
		boundary[value.Role] = value
	}
	return boundary, nil
}

func validateEvolutionBoundaryNode(node nodeEvidence, value evolutionBoundaryNode) error {
	events := make(map[string]eventEvidence, len(node.Events))
	for _, event := range node.Events {
		events[event.ID] = event
	}
	lineage := make(map[string]struct{}, len(node.References))
	for _, reference := range node.References {
		lineage[reference.EventID] = struct{}{}
	}
	for _, head := range value.ActiveHeads {
		event, exists := events[head.EventID]
		_, isReference := lineage[head.EventID]
		if !exists || !isReference || event.Digest != head.EventDigest ||
			event.OriginSequence <= value.ConsolidationAfterSequence ||
			event.OriginSequence > value.MaxOriginSequence {
			return errors.New("evolution boundary head differs from stopped Reference lineage")
		}
	}
	return nil
}

func validateEvolutionEffects(nodes map[string]nodeEvidence,
	boundary map[string]evolutionBoundaryNode, values []evolutionNodeSummary,
) (int, error) {
	total := 0
	for _, reported := range values {
		node, exists := nodes[reported.Role]
		base, hasBoundary := boundary[reported.Role]
		if !exists || !hasBoundary {
			return 0, errors.New("evolution effect names an absent authority boundary")
		}
		expected := collectEvolutionMatches(node, base)
		if err := validateEvolutionMatches(expected, reported.Matches); err != nil {
			return 0, err
		}
		total += len(expected)
	}
	return total, nil
}

func collectEvolutionMatches(node nodeEvidence,
	base evolutionBoundaryNode,
) map[string]evolutionMatchReport {
	expected := make(map[string]evolutionMatchReport)
	for _, event := range node.Events {
		if event.OriginSequence <= base.MaxOriginSequence {
			continue
		}
		for _, head := range base.ActiveHeads {
			if !eventUsesReferenceHead(event, head) {
				continue
			}
			match := evolutionMatchReport{EventID: event.ID,
				ReferenceEventID: head.EventID, ReferenceDigest: head.EventDigest}
			expected[evolutionMatchKey(match)] = match
		}
	}
	return expected
}

func validateEvolutionMatches(expected map[string]evolutionMatchReport,
	reported []evolutionMatchReport,
) error {
	if len(expected) != len(reported) {
		return errors.New("reported evolution effects differ from stopped authority")
	}
	seen := make(map[string]struct{}, len(reported))
	for _, match := range reported {
		key := evolutionMatchKey(match)
		if _, duplicate := seen[key]; duplicate || expected[key] != match {
			return errors.New("reported evolution match is not an exact accepted Event edge")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func eventUsesReferenceHead(event eventEvidence, head evolutionReferenceHead) bool {
	for _, causal := range event.Causation {
		if causal.ID == head.EventID && causal.Digest == head.EventDigest {
			return true
		}
	}
	return event.ReferenceHead == head.EventID && event.ReferenceDigest == head.EventDigest
}

func evolutionMatchKey(match evolutionMatchReport) string {
	return match.EventID + "\x00" + match.ReferenceEventID + "\x00" + match.ReferenceDigest
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
