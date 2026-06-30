package driver

import (
	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

const MulticaDefaultRegistryRelPath = multicasurface.MulticaDefaultRegistryRelPath

type MulticaRegistry = multicasurface.MulticaRegistry
type MulticaParticipantRecord = multicasurface.MulticaParticipantRecord

func MulticaRegistryPath(root, explicit string) string {
	return multicasurface.MulticaRegistryPath(root, explicit)
}

func LoadMulticaRegistry(path string) (MulticaRegistry, bool, error) {
	return multicasurface.LoadMulticaRegistry(path)
}

func SaveMulticaRegistry(path string, reg MulticaRegistry) error {
	return multicasurface.SaveMulticaRegistry(path, reg)
}

func DefaultMulticaParticipantRecords(prefix string) []MulticaParticipantRecord {
	return multicasurface.DefaultMulticaParticipantRecords(prefix)
}

func UpsertMulticaParticipantRecord(records []MulticaParticipantRecord, next MulticaParticipantRecord) []MulticaParticipantRecord {
	return multicasurface.UpsertMulticaParticipantRecord(records, next)
}
