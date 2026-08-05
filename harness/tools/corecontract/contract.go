// Package corecontract validates and executes the tracked R7 evidence ledger.
package corecontract

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

const (
	DocumentPath          = "docs/harness/r7-core-contract.md"
	RegistryPath          = "harness/test/contracts/r7-requirements.json"
	RegistrySchemaVersion = 1
	ReportSchemaVersion   = 1
	InvariantCount        = 10
	GateCount             = 10
)

type Lifecycle string

const (
	LifecycleProposed Lifecycle = "PROPOSED"
	LifecycleActive   Lifecycle = "ACTIVE"
	LifecycleRetired  Lifecycle = "RETIRED"
)

type Contract struct {
	Lifecycle  Lifecycle
	Invariants []string
	Gates      []string
}

var (
	statusPattern    = regexp.MustCompile(`^Status: \*\*(PROPOSED|ACTIVE|RETIRED)\*\*\.`)
	invariantPattern = regexp.MustCompile(`^\*\*(P-[0-9]{2})\s`)
	gatePattern      = regexp.MustCompile(`^\| \x60(G-R7-[A-Z-]+)\x60 \|`)
)

var expectedInvariants = []string{
	"P-01", "P-02", "P-03", "P-04", "P-05",
	"P-06", "P-07", "P-08", "P-09", "P-10",
}

var expectedGates = []string{
	"G-R7-AUTHORITY-CUTOVER",
	"G-R7-CASE-DATA-ONLY",
	"G-R7-CASES",
	"G-R7-CONTINUITY",
	"G-R7-CORE",
	"G-R7-FEDERATION",
	"G-R7-NO-CASE-KIND",
	"G-R7-ONE-PATH",
	"G-R7-PATTERN-FREE",
	"G-R7-ROOT-ISOLATION",
}

func Load(root string) (Contract, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(DocumentPath)))
	if err != nil {
		return Contract{}, fmt.Errorf("read R7 Core contract: %w", err)
	}
	return Parse(data)
}

func Parse(data []byte) (Contract, error) {
	var contract Contract
	invariants := make(map[string]struct{}, InvariantCount)
	gates := make(map[string]struct{}, GateCount)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(strings.TrimSuffix(scanner.Text(), "\r"))
		if match := statusPattern.FindStringSubmatch(line); match != nil {
			if contract.Lifecycle != "" {
				return Contract{}, fmt.Errorf("contract repeats lifecycle status")
			}
			contract.Lifecycle = Lifecycle(match[1])
		}
		if match := invariantPattern.FindStringSubmatch(line); match != nil {
			if _, exists := invariants[match[1]]; exists {
				return Contract{}, fmt.Errorf("contract repeats invariant %s", match[1])
			}
			invariants[match[1]] = struct{}{}
			contract.Invariants = append(contract.Invariants, match[1])
		}
		if match := gatePattern.FindStringSubmatch(line); match != nil {
			if _, exists := gates[match[1]]; exists {
				return Contract{}, fmt.Errorf("contract repeats gate %s", match[1])
			}
			gates[match[1]] = struct{}{}
			contract.Gates = append(contract.Gates, match[1])
		}
	}
	if err := scanner.Err(); err != nil {
		return Contract{}, fmt.Errorf("scan R7 Core contract: %w", err)
	}
	if contract.Lifecycle == "" {
		return Contract{}, fmt.Errorf("contract has no canonical lifecycle status")
	}
	slices.Sort(contract.Invariants)
	slices.Sort(contract.Gates)
	if !slices.Equal(contract.Invariants, expectedInvariants) {
		return Contract{}, fmt.Errorf("contract invariants = %v, want %v",
			contract.Invariants, expectedInvariants)
	}
	if !slices.Equal(contract.Gates, expectedGates) {
		return Contract{}, fmt.Errorf("contract gates = %v, want %v",
			contract.Gates, expectedGates)
	}
	return contract, nil
}

func ValidateAuthorityCutover(root string) error {
	files, err := filepath.Glob(filepath.Join(root, "docs", "harness", "*core-contract.md"))
	if err != nil {
		return fmt.Errorf("enumerate Core contracts: %w", err)
	}
	active := 0
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", filepath.Base(file), err)
		}
		lifecycle, err := parseLifecycle(data)
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(file), err)
		}
		if lifecycle == LifecycleActive {
			active++
		}
	}
	if active != 1 {
		return fmt.Errorf("tracked Core contracts have %d ACTIVE markers, want exactly one", active)
	}
	contract, err := Load(root)
	if err != nil {
		return err
	}
	if contract.Lifecycle != LifecycleActive {
		return fmt.Errorf("R7 Core lifecycle = %s, want ACTIVE", contract.Lifecycle)
	}
	return nil
}

func parseLifecycle(data []byte) (Lifecycle, error) {
	var lifecycle Lifecycle
	for _, raw := range bytes.Split(data, []byte("\n")) {
		match := statusPattern.FindStringSubmatch(strings.TrimSpace(string(raw)))
		if match == nil {
			continue
		}
		if lifecycle != "" {
			return "", fmt.Errorf("repeats lifecycle status")
		}
		lifecycle = Lifecycle(match[1])
	}
	if lifecycle == "" {
		return "", fmt.Errorf("missing lifecycle status")
	}
	return lifecycle, nil
}
