package multica

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const MulticaDefaultRegistryRelPath = ".mnemon/harness/multica/registry.json"

type MulticaRegistry struct {
	SchemaVersion    int                        `json:"schema_version"`
	WorkspaceID      string                     `json:"workspace_id"`
	RuntimeProfileID string                     `json:"runtime_profile_id"`
	RuntimeID        string                     `json:"runtime_id"`
	Participants     []MulticaParticipantRecord `json:"participants"`
}

type MulticaParticipantRecord struct {
	Principal string `json:"principal"`
	AgentName string `json:"multica_agent_name"`
	AgentID   string `json:"multica_agent_id"`
	Role      string `json:"role"`
}

func MulticaRegistryPath(root, explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return strings.TrimSpace(explicit)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		root = "."
	}
	return filepath.Join(root, MulticaDefaultRegistryRelPath)
}

func LoadMulticaRegistry(path string) (MulticaRegistry, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return MulticaRegistry{}, false, nil
	}
	if err != nil {
		return MulticaRegistry{}, false, err
	}
	var reg MulticaRegistry
	if err := json.Unmarshal(data, &reg); err != nil {
		return MulticaRegistry{}, false, err
	}
	return reg, true, nil
}

func SaveMulticaRegistry(path string, reg MulticaRegistry) error {
	if reg.SchemaVersion == 0 {
		reg.SchemaVersion = 1
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

func DefaultMulticaParticipantRecords(prefix string) []MulticaParticipantRecord {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "mnemon"
	}
	roles := []string{"planner", "researcher", "implementer", "reviewer", "integrator"}
	out := make([]MulticaParticipantRecord, 0, len(roles))
	for _, role := range roles {
		out = append(out, MulticaParticipantRecord{
			Principal: role + "@team",
			AgentName: prefix + "-" + role,
			Role:      role,
		})
	}
	return out
}

func UpsertMulticaParticipantRecord(records []MulticaParticipantRecord, next MulticaParticipantRecord) []MulticaParticipantRecord {
	for i := range records {
		if records[i].Principal == next.Principal || records[i].AgentName == next.AgentName {
			records[i] = next
			return records
		}
	}
	return append(records, next)
}

func MulticaParticipantForPrincipal(reg MulticaRegistry, principal string) (MulticaParticipantRecord, bool) {
	principal = strings.TrimSpace(principal)
	for _, participant := range reg.Participants {
		if strings.TrimSpace(participant.Principal) == principal {
			return participant, true
		}
	}
	return MulticaParticipantRecord{}, false
}

func MulticaPrincipalForAgent(reg MulticaRegistry, agentID, agentName string) string {
	agentID = strings.TrimSpace(agentID)
	agentName = strings.TrimSpace(agentName)
	for _, participant := range reg.Participants {
		principal := strings.TrimSpace(participant.Principal)
		if principal == "" {
			continue
		}
		if agentID != "" && strings.TrimSpace(participant.AgentID) == agentID {
			return principal
		}
		if agentName != "" && strings.TrimSpace(participant.AgentName) == agentName {
			return principal
		}
	}
	return ""
}

func RuntimeMulticaRegistryPrincipal(env []string, cwd, agentID, agentName string) string {
	for _, path := range RuntimeMulticaRegistryPaths(env, cwd) {
		reg, ok, err := LoadMulticaRegistry(path)
		if err != nil || !ok {
			continue
		}
		if principal := MulticaPrincipalForAgent(reg, agentID, agentName); principal != "" {
			return principal
		}
	}
	return ""
}
