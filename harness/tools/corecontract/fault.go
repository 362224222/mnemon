package corecontract

import (
	"fmt"
	"path/filepath"
	"slices"
	"time"
)

type runtimeFaultReport struct {
	ID                  string             `json:"id"`
	PublicPrecondition  runtimeFaultStage  `json:"public_precondition"`
	ExternalAction      runtimeFaultAction `json:"external_action"`
	PublicPostcondition runtimeFaultStage  `json:"public_postcondition"`
}

type runtimeFaultStage struct {
	Passed             *bool    `json:"passed"`
	At                 string   `json:"at"`
	ObservationReceipt string   `json:"observation_receipt"`
	EvidenceRefs       []string `json:"evidence_refs"`
}

type runtimeFaultAction struct {
	Applied       *bool    `json:"applied"`
	At            string   `json:"at"`
	ActionReceipt string   `json:"action_receipt"`
	EvidenceRefs  []string `json:"evidence_refs"`
}

type runtimeObservationReceipt struct {
	FaultID           string `json:"fault_id"`
	Phase             string `json:"phase"`
	At                string `json:"at"`
	PublicObservation bool   `json:"public_observation"`
}

type runtimeActionReceipt struct {
	ExternalActionApplied bool   `json:"external_action_applied"`
	FaultID               string `json:"fault_id"`
	ExternalAction        string `json:"external_action"`
	At                    string `json:"at"`
}

func (bundle loadedRuntimeBundle) validateFault(casePath string, report runtimeCaseReport,
	id string,
) (bool, string, error) {
	ok, reason, err := bundle.validateAssertion(casePath, report, "fault-"+id, "system")
	if err != nil || !ok {
		return ok, reason, err
	}
	for _, raw := range report.Faults {
		var fault runtimeFaultReport
		if err := decodeStrictJSON(raw, &fault); err != nil {
			return false, "", fmt.Errorf("decode fault runtime evidence: %w", err)
		}
		if fault.ID == id {
			return bundle.validateFaultSequence(casePath, fault)
		}
	}
	return false, "fault runtime evidence is absent: " + id, nil
}

func (bundle loadedRuntimeBundle) validateFaultSequence(casePath string,
	fault runtimeFaultReport,
) (bool, string, error) {
	pre, action, post := fault.PublicPrecondition, fault.ExternalAction,
		fault.PublicPostcondition
	if !validFaultStage(pre) || !validFaultAction(action) || !validFaultStage(post) ||
		pre.ObservationReceipt == post.ObservationReceipt {
		return false, "fault lacks public precondition, external action, receipt, or postcondition", nil
	}
	if !validFaultTimeline(pre.At, action.At, post.At) {
		return false, "fault precondition, action, and postcondition timestamps are invalid", nil
	}
	refs := append(append([]string{}, pre.EvidenceRefs...), action.EvidenceRefs...)
	refs = append(refs, post.EvidenceRefs...)
	if err := bundle.verifyEvidenceRefs(casePath, refs); err != nil {
		return false, "", err
	}
	if !bundle.observationReceiptBinds(casePath, fault.ID, "precondition", pre) ||
		!bundle.observationReceiptBinds(casePath, fault.ID, "postcondition", post) {
		return false, "fault observation receipt does not bind its public stage", nil
	}
	if !bundle.actionReceiptBinds(casePath, fault.ID, action) {
		return false, "fault action receipt does not prove an external action", nil
	}
	return true, "", nil
}

func validFaultStage(stage runtimeFaultStage) bool {
	return stage.Passed != nil && *stage.Passed && len(stage.EvidenceRefs) > 0 &&
		stage.ObservationReceipt != "" &&
		slices.Contains(stage.EvidenceRefs, stage.ObservationReceipt)
}

func validFaultAction(action runtimeFaultAction) bool {
	return action.Applied != nil && *action.Applied && len(action.EvidenceRefs) > 0 &&
		action.ActionReceipt != "" &&
		slices.Contains(action.EvidenceRefs, action.ActionReceipt)
}

func validFaultTimeline(preValue, actionValue, postValue string) bool {
	pre, preErr := time.Parse(time.RFC3339Nano, preValue)
	action, actionErr := time.Parse(time.RFC3339Nano, actionValue)
	post, postErr := time.Parse(time.RFC3339Nano, postValue)
	return preErr == nil && actionErr == nil && postErr == nil &&
		pre.Before(action) && action.Before(post)
}

func (bundle loadedRuntimeBundle) observationReceiptBinds(casePath, faultID, phase string,
	stage runtimeFaultStage,
) bool {
	data, err := bundle.readVerifiedFile(filepath.ToSlash(
		filepath.Join(casePath, stage.ObservationReceipt)))
	var proof runtimeObservationReceipt
	if err != nil || decodeStrictJSON(data, &proof) != nil {
		return false
	}
	return proof.PublicObservation && proof.FaultID == faultID &&
		proof.Phase == phase && proof.At == stage.At
}

func (bundle loadedRuntimeBundle) actionReceiptBinds(casePath, faultID string,
	action runtimeFaultAction,
) bool {
	data, err := bundle.readVerifiedFile(filepath.ToSlash(
		filepath.Join(casePath, action.ActionReceipt)))
	var proof runtimeActionReceipt
	if err != nil || decodeStrictJSON(data, &proof) != nil {
		return false
	}
	return proof.ExternalActionApplied && proof.FaultID == faultID &&
		proof.ExternalAction != "" && proof.At == action.At
}
