package driver

import (
	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

const (
	MulticaHubBackend = multicasurface.MulticaHubBackend

	MulticaDefaultHubLedgerRelPath = multicasurface.MulticaDefaultHubLedgerRelPath

	MulticaMetadataSchemaVersion          = multicasurface.MulticaMetadataSchemaVersion
	MulticaMetadataHubBackend             = multicasurface.MulticaMetadataHubBackend
	MulticaMetadataKind                   = multicasurface.MulticaMetadataKind
	MulticaMetadataSessionID              = multicasurface.MulticaMetadataSessionID
	MulticaMetadataCorrelationID          = multicasurface.MulticaMetadataCorrelationID
	MulticaMetadataEventID                = multicasurface.MulticaMetadataEventID
	MulticaMetadataEventType              = multicasurface.MulticaMetadataEventType
	MulticaMetadataEventPhase             = multicasurface.MulticaMetadataEventPhase
	MulticaMetadataAssignmentID           = multicasurface.MulticaMetadataAssignmentID
	MulticaMetadataAssignmentFingerprint  = multicasurface.MulticaMetadataAssignmentFingerprint
	MulticaMetadataPrincipal              = multicasurface.MulticaMetadataPrincipal
	MulticaMetadataSourceIssueID          = multicasurface.MulticaMetadataSourceIssueID
	MulticaMetadataRootIssueID            = multicasurface.MulticaMetadataRootIssueID
	MulticaMetadataProjectionOwner        = multicasurface.MulticaMetadataProjectionOwner
	MulticaMetadataMulticaAgentID         = multicasurface.MulticaMetadataMulticaAgentID
	MulticaMetadataProjectedAt            = multicasurface.MulticaMetadataProjectedAt
	MulticaMetadataEnvelopeDigest         = multicasurface.MulticaMetadataEnvelopeDigest
	MulticaHubKindSession                 = multicasurface.MulticaHubKindSession
	MulticaHubKindAssignmentMailbox       = multicasurface.MulticaHubKindAssignmentMailbox
	MulticaHubKindFeedbackCarrier         = multicasurface.MulticaHubKindFeedbackCarrier
	MulticaHubKindAssignmentProjectionOld = multicasurface.MulticaHubKindAssignmentProjectionOld
)

type MulticaHubMetadata = multicasurface.MulticaHubMetadata
type MulticaAssignmentFingerprintInput = multicasurface.MulticaAssignmentFingerprintInput
type MulticaHubLedgerRecord = multicasurface.MulticaHubLedgerRecord
type MulticaHubLedgerSource = multicasurface.MulticaHubLedgerSource
type MulticaHubLedgerTarget = multicasurface.MulticaHubLedgerTarget
type FileMulticaHubLedger = multicasurface.FileMulticaHubLedger

func MulticaHubLedgerPath(root, explicit string) string {
	return multicasurface.MulticaHubLedgerPath(root, explicit)
}

func NewFileMulticaHubLedger(path string) *FileMulticaHubLedger {
	return multicasurface.NewFileMulticaHubLedger(path)
}

func ParseMulticaHubMetadata(raw map[string]any) MulticaHubMetadata {
	return multicasurface.ParseMulticaHubMetadata(raw)
}

func MulticaIssueHubMetadata(issue MulticaIssue) MulticaHubMetadata {
	return ParseMulticaHubMetadata(issue.Metadata)
}

func IsMulticaAssignmentMailboxIssue(issue MulticaIssue) bool {
	return MulticaIssueHubMetadata(issue).IsAssignmentMailbox()
}

func NormalizeMulticaMetadata(raw any) map[string]string {
	return multicasurface.NormalizeMulticaMetadata(raw)
}

func MulticaAssignmentFingerprint(input MulticaAssignmentFingerprintInput) string {
	return multicasurface.MulticaAssignmentFingerprint(input)
}

func MulticaSessionID(rootIssueID string) string {
	return multicasurface.MulticaSessionID(rootIssueID)
}
