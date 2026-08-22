package setup

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/internal/memory/setup/assets"
)

// MiniMaxCodeWriteSkill writes the Mnemon skill to a MiniMax Code skill root.
func MiniMaxCodeWriteSkill(configDir string) (string, error) {
	skillDir := filepath.Join(configDir, "skills", "mnemon")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		return "", err
	}
	skillPath := filepath.Join(skillDir, "SKILL.md")
	if err := os.WriteFile(skillPath, assets.MiniMaxCodeSkill, 0o644); err != nil {
		return "", err
	}
	return skillPath, nil
}

// MiniMaxCodeEject removes only the Mnemon skill from a MiniMax Code config root.
func MiniMaxCodeEject(configDir string) []error {
	var errs []error

	fmt.Printf("\nRemoving MiniMax Code integration (%s)...\n", configDir)

	skillDir := filepath.Join(configDir, "skills", "mnemon")
	if err := os.RemoveAll(skillDir); err != nil {
		StatusError(1, 1, "Skill", err)
		errs = append(errs, err)
	} else {
		StatusOK(1, 1, "Skill", skillDir+" removed")
	}
	removeIfEmpty(filepath.Join(configDir, "skills"))
	removeIfEmpty(configDir)

	return errs
}
