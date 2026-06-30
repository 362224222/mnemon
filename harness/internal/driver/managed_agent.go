package driver

import (
	"github.com/mnemon-dev/mnemon/harness/internal/drive"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

const ManagedWakeQuery = drive.ManagedWakeQuery

type ManagedTurnClient = drive.ManagedTurnClient
type ManagedTurnResult = drive.ManagedTurnResult
type ManagedWakeCandidate = drive.ManagedWakeCandidate
type ManagedWakeRecord = drive.ManagedWakeRecord
type ManagedWakeLedger = drive.ManagedWakeLedger
type MemoryManagedWakeLedger = drive.MemoryManagedWakeLedger
type ManagedAgentDriver = drive.ManagedAgentDriver

func NewMemoryManagedWakeLedger() *MemoryManagedWakeLedger {
	return drive.NewMemoryManagedWakeLedger()
}

func ManagedWakeCandidatesFromEvents(principal string, events []eventmodel.EventEnvelope) []ManagedWakeCandidate {
	return drive.ManagedWakeCandidatesFromEvents(principal, events)
}
