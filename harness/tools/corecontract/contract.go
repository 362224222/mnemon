// Package corecontract loads the tracked R5 Core requirement and gate authority.
package corecontract

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	DocumentPath     = "docs/harness/r5-core-contract.md"
	RequirementCount = 42
	GateCount        = 7
)

var (
	requirementIDPattern = regexp.MustCompile(`^[A-Z]{2}-[0-9]{2}$`)
	gateIDPattern        = regexp.MustCompile(`^G-[A-Z]+$`)
)

type Contract struct {
	Requirements []Requirement
	Gates        []Gate
}

type Requirement struct {
	ID          string
	Level       string
	Clause      string
	Owner       string
	PrimaryGate string
}

type Gate struct {
	ID      string
	Closure string
}

func Load(root string) (Contract, error) {
	contents, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(DocumentPath)))
	if err != nil {
		return Contract{}, fmt.Errorf("read tracked R5 Core contract: %w", err)
	}
	return Parse(contents)
}

func Parse(contents []byte) (Contract, error) {
	var contract Contract
	section := ""
	requirementsSection := false
	gatesSection := false
	requirementIDs := make(map[string]struct{}, RequirementCount)
	gateIDs := make(map[string]struct{}, GateCount)
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		trimmed := strings.TrimSpace(line)
		switch trimmed {
		case "## 8. Canonical requirements":
			if requirementsSection {
				return Contract{}, fmt.Errorf("tracked contract repeats canonical requirements section")
			}
			requirementsSection = true
			section = "requirements"
			continue
		case "## 9. Closed evidence gates":
			if gatesSection {
				return Contract{}, fmt.Errorf("tracked contract repeats closed evidence gates section")
			}
			gatesSection = true
			section = "gates"
			continue
		}
		if strings.HasPrefix(trimmed, "## ") {
			section = ""
			continue
		}
		switch section {
		case "requirements":
			requirement, parsed, err := parseRequirementRow(trimmed)
			if err != nil {
				return Contract{}, err
			}
			if !parsed {
				continue
			}
			if _, exists := requirementIDs[requirement.ID]; exists {
				return Contract{}, fmt.Errorf("tracked contract repeats requirement %s", requirement.ID)
			}
			requirementIDs[requirement.ID] = struct{}{}
			contract.Requirements = append(contract.Requirements, requirement)
		case "gates":
			gate, parsed, err := parseGateRow(trimmed)
			if err != nil {
				return Contract{}, err
			}
			if !parsed {
				continue
			}
			if _, exists := gateIDs[gate.ID]; exists {
				return Contract{}, fmt.Errorf("tracked contract repeats gate %s", gate.ID)
			}
			gateIDs[gate.ID] = struct{}{}
			contract.Gates = append(contract.Gates, gate)
		}
	}
	if err := scanner.Err(); err != nil {
		return Contract{}, fmt.Errorf("scan tracked R5 Core contract: %w", err)
	}
	if !requirementsSection || len(contract.Requirements) != RequirementCount {
		return Contract{}, fmt.Errorf("tracked contract has %d requirements, want %d",
			len(contract.Requirements), RequirementCount)
	}
	if !gatesSection || len(contract.Gates) != GateCount {
		return Contract{}, fmt.Errorf("tracked contract has %d gates, want %d",
			len(contract.Gates), GateCount)
	}
	for _, requirement := range contract.Requirements {
		if _, exists := gateIDs[requirement.PrimaryGate]; !exists {
			return Contract{}, fmt.Errorf("requirement %s references unknown primary gate %s",
				requirement.ID, requirement.PrimaryGate)
		}
	}
	return contract, nil
}

func (contract Contract) RequirementByID() map[string]Requirement {
	requirements := make(map[string]Requirement, len(contract.Requirements))
	for _, requirement := range contract.Requirements {
		requirements[requirement.ID] = requirement
	}
	return requirements
}

func (contract Contract) GateIDs() []string {
	ids := make([]string, len(contract.Gates))
	for index, gate := range contract.Gates {
		ids[index] = gate.ID
	}
	sort.Strings(ids)
	return ids
}

func ValidateOwnerDirectories(root string, contract Contract) error {
	for _, requirement := range contract.Requirements {
		info, err := os.Stat(filepath.Join(root, filepath.FromSlash(requirement.Owner)))
		if err != nil || !info.IsDir() {
			return fmt.Errorf("requirement %s owner %q is not a repository directory",
				requirement.ID, requirement.Owner)
		}
	}
	return nil
}

func parseRequirementRow(line string) (Requirement, bool, error) {
	cells, row, err := markdownRow(line, 5)
	if err != nil || !row {
		return Requirement{}, false, err
	}
	if cells[0] == "ID" || dividerCell(cells[0]) {
		return Requirement{}, false, nil
	}
	if !requirementIDPattern.MatchString(cells[0]) {
		return Requirement{}, false, fmt.Errorf("tracked contract has invalid requirement ID %q", cells[0])
	}
	if cells[1] != "MUST" {
		return Requirement{}, false, fmt.Errorf("requirement %s has non-Core level %q", cells[0], cells[1])
	}
	if cells[2] == "" {
		return Requirement{}, false, fmt.Errorf("requirement %s has an empty canonical clause", cells[0])
	}
	owner, err := codeSpan(cells[3])
	if err != nil {
		return Requirement{}, false, fmt.Errorf("requirement %s owner: %w", cells[0], err)
	}
	if err := validateOwnerPath(owner); err != nil {
		return Requirement{}, false, fmt.Errorf("requirement %s owner: %w", cells[0], err)
	}
	fields := strings.Fields(cells[4])
	if len(fields) < 2 {
		return Requirement{}, false, fmt.Errorf("requirement %s has malformed evidence cell", cells[0])
	}
	gate, err := codeSpan(fields[0])
	if err != nil || !gateIDPattern.MatchString(gate) {
		return Requirement{}, false, fmt.Errorf("requirement %s has malformed primary gate %q",
			cells[0], fields[0])
	}
	return Requirement{
		ID: cells[0], Level: cells[1], Clause: cells[2], Owner: owner, PrimaryGate: gate,
	}, true, nil
}

func parseGateRow(line string) (Gate, bool, error) {
	cells, row, err := markdownRow(line, 2)
	if err != nil || !row {
		return Gate{}, false, err
	}
	if cells[0] == "Gate" || dividerCell(cells[0]) {
		return Gate{}, false, nil
	}
	id, err := codeSpan(cells[0])
	if err != nil || !gateIDPattern.MatchString(id) {
		return Gate{}, false, fmt.Errorf("tracked contract has malformed gate %q", cells[0])
	}
	if cells[1] == "" {
		return Gate{}, false, fmt.Errorf("gate %s has an empty closure rule", id)
	}
	return Gate{ID: id, Closure: cells[1]}, true, nil
}

func markdownRow(line string, columns int) ([]string, bool, error) {
	if !strings.HasPrefix(line, "|") {
		return nil, false, nil
	}
	if !strings.HasSuffix(line, "|") {
		return nil, false, fmt.Errorf("malformed Markdown table row %q", line)
	}
	parts := strings.Split(line[1:len(line)-1], "|")
	if len(parts) != columns {
		return nil, false, fmt.Errorf("Markdown table row has %d cells, want %d", len(parts), columns)
	}
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	return parts, true, nil
}

func codeSpan(value string) (string, error) {
	if len(value) < 3 || value[0] != '`' || value[len(value)-1] != '`' ||
		strings.Contains(value[1:len(value)-1], "`") {
		return "", fmt.Errorf("%q is not one exact code span", value)
	}
	return value[1 : len(value)-1], nil
}

func dividerCell(value string) bool {
	value = strings.Trim(value, " :-")
	return value == ""
}

func validateOwnerPath(owner string) error {
	if owner == "" || owner == "." || strings.Contains(owner, "\\") || path.IsAbs(owner) ||
		path.Clean(owner) != owner || strings.HasPrefix(owner, "../") {
		return fmt.Errorf("%q is not a clean repository-relative directory", owner)
	}
	return nil
}
