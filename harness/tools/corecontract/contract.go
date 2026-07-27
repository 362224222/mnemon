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
	gitObjectPattern     = regexp.MustCompile(`^[0-9a-f]{40}$`)
	digestPattern        = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	runIDPattern         = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$`)
)

const GateReportSchemaVersion = 1

const (
	RequirementPending  = "pending"
	RequirementVerified = "verified"
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

type GateReport struct {
	SchemaVersion int             `json:"schema_version"`
	RunID         string          `json:"run_id"`
	StartedAt     string          `json:"started_at"`
	FinishedAt    string          `json:"finished_at"`
	Source        GateSource      `json:"source"`
	Inputs        GateInputs      `json:"inputs"`
	Steps         []GateStep      `json:"steps"`
	Bundles       []GateBundleRef `json:"bundles"`
}

type GateSource struct {
	Commit        string `json:"commit"`
	Tree          string `json:"tree"`
	CleanAtStart  bool   `json:"clean_at_start"`
	CleanAtFinish bool   `json:"clean_at_finish"`
}

type GateInputs struct {
	ContractSHA256     string `json:"contract_sha256"`
	RequirementsSHA256 string `json:"requirements_sha256"`
}

type GateStep struct {
	ID         string     `json:"id"`
	Gate       string     `json:"gate"`
	Kind       string     `json:"kind"`
	Argv       []string   `json:"argv"`
	StartedAt  string     `json:"started_at"`
	FinishedAt string     `json:"finished_at"`
	ExitCode   int        `json:"exit_code"`
	Output     GateOutput `json:"output"`
}

type GateOutput struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type GateBundleRef struct {
	Runtime        string `json:"runtime"`
	RunID          string `json:"run_id"`
	ReportPath     string `json:"report_path"`
	ReportSHA256   string `json:"report_sha256"`
	ManifestPath   string `json:"manifest_path"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type Closure struct {
	Requirements []RequirementResult
	Gates        []GateResult
}

type RequirementResult struct {
	ID, Gate, Status string
	Proofs           []string
	Reason           string
}

type GateResult struct {
	ID, Status, Reason string
}

type stepRule struct {
	id, gate, kind string
	argv           []string
}

var gateStepRules = []stepRule{
	{"contract", "G-CONTRACT", "go-test", []string{"go", "test", "-json",
		"./harness/tools/corecontract", "./harness/test/contracts", "-count=1"}},
	{"quality", "G-CONTRACT", "command", []string{"make", "harness-quality"}},
	{"root-build", "G-ROOT", "command", []string{"go", "build", "./..."}},
	{"root-unit", "G-ROOT", "go-test", []string{"go", "test", "-json",
		"./cmd/...", "./internal/...", "-count=1"}},
	{"legacy-e2e", "G-ROOT", "command", []string{"bash", "scripts/e2e_test.sh"}},
	{"harness-build", "G-UNIT", "command", []string{"go", "build", "./harness/cmd/..."}},
	{"harness-unit", "G-UNIT", "go-test", []string{"go", "test", "-json",
		"./harness/cmd/...", "./harness/internal/...", "./harness/tools/...",
		"./harness/test/contracts", "-count=1"}},
	{"harness-race", "G-UNIT", "go-test", []string{"go", "test", "-json", "-race",
		"./harness/cmd/...", "./harness/internal/...", "./harness/tools/...",
		"./harness/test/contracts", "-count=1"}},
	{"fuzz-model", "G-UNIT", "go-fuzz", []string{"go", "test", "-json",
		"./harness/internal/model", "-run", "^$", "-fuzz",
		"^FuzzParseSignedPublication$", "-fuzztime=100x"}},
	{"fuzz-peer", "G-UNIT", "go-fuzz", []string{"go", "test", "-json",
		"./harness/internal/peer", "-run", "^$", "-fuzz",
		"^FuzzReadChannelFrame$", "-fuzztime=100x"}},
	{"fuzz-artifact", "G-UNIT", "go-fuzz", []string{"go", "test", "-json",
		"./harness/internal/artifact", "-run", "^$", "-fuzz",
		"^FuzzParseManifest$", "-fuzztime=100x"}},
	{"process", "G-PROCESS", "go-test", []string{"go", "test", "-json",
		"./harness/test/process", "-count=1"}},
	{"docker", "G-DOCKER", "command",
		[]string{"harness/test/e2e/runner/run_docker.sh"}},
	{"evidence-hermetic", "G-EVIDENCE", "evidence", nil},
	{"live", "G-LIVE", "command",
		[]string{"harness/test/e2e/runner/run_live_codex.sh"}},
	{"evidence-live", "G-LIVE", "evidence", nil},
}

type contractSections struct {
	current      string
	requirements bool
	gates        bool
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
	var sections contractSections
	requirementIDs := make(map[string]struct{}, RequirementCount)
	gateIDs := make(map[string]struct{}, GateCount)
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	for scanner.Scan() {
		line := strings.TrimSpace(strings.TrimSuffix(scanner.Text(), "\r"))
		handled, err := sections.consume(line)
		if err != nil {
			return Contract{}, err
		}
		if handled {
			continue
		}
		switch sections.current {
		case "requirements":
			if err := appendRequirementRow(&contract, requirementIDs, line); err != nil {
				return Contract{}, err
			}
		case "gates":
			if err := appendGateRow(&contract, gateIDs, line); err != nil {
				return Contract{}, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return Contract{}, fmt.Errorf("scan tracked R5 Core contract: %w", err)
	}
	if !sections.requirements || len(contract.Requirements) != RequirementCount {
		return Contract{}, fmt.Errorf("tracked contract has %d requirements, want %d",
			len(contract.Requirements), RequirementCount)
	}
	if !sections.gates || len(contract.Gates) != GateCount {
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

func (sections *contractSections) consume(line string) (bool, error) {
	switch line {
	case "## 8. Canonical requirements":
		if sections.requirements {
			return true, fmt.Errorf("tracked contract repeats canonical requirements section")
		}
		sections.requirements = true
		sections.current = "requirements"
		return true, nil
	case "## 9. Closed evidence gates":
		if sections.gates {
			return true, fmt.Errorf("tracked contract repeats closed evidence gates section")
		}
		sections.gates = true
		sections.current = "gates"
		return true, nil
	}
	if strings.HasPrefix(line, "## ") {
		sections.current = ""
		return true, nil
	}
	return false, nil
}

func appendRequirementRow(contract *Contract, ids map[string]struct{}, line string) error {
	requirement, parsed, err := parseRequirementRow(line)
	if err != nil || !parsed {
		return err
	}
	if _, exists := ids[requirement.ID]; exists {
		return fmt.Errorf("tracked contract repeats requirement %s", requirement.ID)
	}
	ids[requirement.ID] = struct{}{}
	contract.Requirements = append(contract.Requirements, requirement)
	return nil
}

func appendGateRow(contract *Contract, ids map[string]struct{}, line string) error {
	gate, parsed, err := parseGateRow(line)
	if err != nil || !parsed {
		return err
	}
	if _, exists := ids[gate.ID]; exists {
		return fmt.Errorf("tracked contract repeats gate %s", gate.ID)
	}
	ids[gate.ID] = struct{}{}
	contract.Gates = append(contract.Gates, gate)
	return nil
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

func fileDigest(filename string) (string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return bytesDigest(data), nil
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
