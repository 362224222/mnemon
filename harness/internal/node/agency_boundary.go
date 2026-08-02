package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/authority"
)

var (
	ErrAgencyAttachmentInput = errors.New("Agency attachment input is invalid")
	ErrAgencyCurrentInput    = errors.New("Agency current operation input is invalid")
)

const (
	AgencyViewSchema               = agency.AgentViewSchema
	AgencyViewVersion              = agency.AgentViewVersion
	AgencyReceiptSchema            = agency.AgentReceiptSchema
	AgencyReceiptVersion           = agency.AgentReceiptVersion
	MaxAgencyViewCanonicalBytes    = agency.MaxAgentViewCanonicalBytes
	MaxAgencyReceiptCanonicalBytes = agency.MaxAgentReceiptCanonicalBytes
	MaxAgencyArtifactInputs        = agency.MaxArtifactInputs
	MaxAgencyArtifactBytes         = authority.MaxArtifactBytes
)

// AgencyAuthority is an opaque, machine-only proof plus the exact Current
// operation that produced the View. Transports can construct it from private
// headers but cannot inspect or alter the resolved Principal or authority.
type AgencyAuthority struct {
	proof   authority.AttachmentProof
	current authority.CurrentOperation
}

// AgencySubmission is the parsed, closed Intent plus its machine-held
// admission operation and candidate bindings. Parsing it creates no durable
// Receipt and performs no state mutation.
type AgencySubmission struct {
	operation  agency.OperationKey
	intent     agency.AgentIntent
	candidates []AgencyCapturedCandidate
}

type AgencyCapturedCandidate struct {
	handle agency.OpaqueHandle
	digest agency.Digest
}

type AgencyErrorClass uint8

const (
	AgencyErrorInternal AgencyErrorClass = iota
	AgencyErrorAuthentication
	AgencyErrorContextStale
	AgencyErrorOperationConflict
	AgencyErrorArtifact
	AgencyErrorLimit
	AgencyErrorInvalid
	AgencyErrorActionNotAllowed
	AgencyErrorUnavailable
)

func NewAgencyAuthority(attachment string, credential []byte,
	currentOperation string,
) (AgencyAuthority, error) {
	id, err := agency.NewAttachmentID(attachment)
	if err != nil {
		return AgencyAuthority{}, ErrAgencyAttachmentInput
	}
	proof, err := authority.NewAttachmentProof(id, credential)
	if err != nil {
		return AgencyAuthority{}, ErrAgencyAttachmentInput
	}
	key, err := agency.NewOperationKey(currentOperation)
	if err != nil {
		return AgencyAuthority{}, ErrAgencyCurrentInput
	}
	current, err := authority.NewCurrentOperation(key)
	if err != nil {
		return AgencyAuthority{}, ErrAgencyCurrentInput
	}
	return AgencyAuthority{proof: proof, current: current}, nil
}

func NewAgencySubmission(operation string, intentJSON []byte,
	bindings []AgencyCandidateBinding,
) (AgencySubmission, error) {
	key, err := agency.NewOperationKey(operation)
	if err != nil {
		return AgencySubmission{}, errors.New("Agency operation is invalid")
	}
	intent, err := agency.ParseAgentIntentJSON(intentJSON)
	if err != nil {
		return AgencySubmission{}, errors.New("Intent is not canonical or valid")
	}
	candidates, err := parseAgencyCandidateBindings(bindings)
	if err != nil {
		return AgencySubmission{}, err
	}
	return AgencySubmission{operation: key, intent: intent, candidates: candidates}, nil
}

func parseAgencyCandidateBindings(bindings []AgencyCandidateBinding) ([]AgencyCapturedCandidate, error) {
	if len(bindings) > agency.MaxArtifactInputs {
		return nil, fmt.Errorf("candidate count exceeds %d", agency.MaxArtifactInputs)
	}
	result := make([]AgencyCapturedCandidate, len(bindings))
	seen := make(map[string]struct{}, len(bindings))
	for index, binding := range bindings {
		handle, handleErr := agency.NewOpaqueHandle(binding.Handle)
		digest, digestErr := agency.ParseDigest(binding.Digest)
		if handleErr != nil || digestErr != nil {
			return nil, errors.New("candidate binding is invalid")
		}
		if _, duplicate := seen[handle.String()]; duplicate {
			return nil, errors.New("candidate binding is duplicated")
		}
		seen[handle.String()] = struct{}{}
		result[index] = AgencyCapturedCandidate{handle: handle, digest: digest}
	}
	return result, nil
}

func ValidateAgencyAttachment(attachment AgencyAttachment) error {
	if _, err := agency.NewAttachmentID(attachment.ID); err != nil ||
		len(attachment.Credential) != 32 || attachment.ExpiresAt.IsZero() {
		return errors.New("Agency attachment is invalid")
	}
	return nil
}

func ValidateAgencyArtifactCapture(capture AgencyArtifactCapture) error {
	handle, handleErr := agency.NewOpaqueHandle(capture.Handle)
	digest, digestErr := agency.ParseDigest(capture.Digest)
	if handleErr != nil || digestErr != nil || handle.IsZero() || digest.IsZero() ||
		capture.ByteSize < 0 || capture.ByteSize > authority.MaxArtifactBytes {
		return errors.New("Agency Artifact capture is invalid")
	}
	return nil
}

func AgencyContentDigest(content []byte) string { return agency.Sum(content).String() }

// ProjectAgencyView and ProjectAgencyReceipt cross the semantic-to-transport
// boundary without exposing machine authority. Their inputs can only be
// constructed by the agency package's closed validators.
func ProjectAgencyView(view agency.AgentView) (AgencyView, error) {
	canonical := view.CanonicalJSON()
	if len(canonical) == 0 || len(canonical) > agency.MaxAgentViewCanonicalBytes {
		return AgencyView{}, errors.New("Agency View projection is invalid")
	}
	return AgencyView{canonical: canonical}, nil
}

func ProjectAgencyReceipt(receipt agency.AgentReceipt) (AgencyReceipt, error) {
	canonical := receipt.CanonicalJSON()
	if len(canonical) == 0 || len(canonical) > agency.MaxAgentReceiptCanonicalBytes {
		return AgencyReceipt{}, errors.New("Agency Receipt projection is invalid")
	}
	return AgencyReceipt{canonical: canonical}, nil
}

func ClassifyAgencyError(err error) AgencyErrorClass {
	switch {
	case err == nil:
		return AgencyErrorInternal
	case errors.Is(err, authority.ErrAttachmentAuth),
		errors.Is(err, authority.ErrPrincipalUnavailable):
		return AgencyErrorAuthentication
	case errors.Is(err, authority.ErrAttachmentExpired),
		errors.Is(err, authority.ErrCurrentUnavailable):
		return AgencyErrorContextStale
	case errors.Is(err, authority.ErrOperationConflict):
		return AgencyErrorOperationConflict
	case errors.Is(err, authority.ErrArtifactUnavailable):
		return AgencyErrorArtifact
	case errors.Is(err, agency.ErrLimit):
		return AgencyErrorLimit
	case errors.Is(err, agency.ErrInvalid):
		return AgencyErrorInvalid
	case errors.Is(err, agency.ErrInvariant):
		return AgencyErrorActionNotAllowed
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, authority.ErrClosed), errors.Is(err, ErrManagedAdmission):
		return AgencyErrorUnavailable
	default:
		return AgencyErrorInternal
	}
}
