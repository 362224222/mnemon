package corecontract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

type Registry struct {
	SchemaVersion int                `json:"schema_version"`
	Invariants    []InvariantBinding `json:"invariants"`
	Gates         []GateBinding      `json:"gates"`
}

type InvariantBinding struct {
	ID      string            `json:"id"`
	Oracles []InvariantOracle `json:"oracles"`
}

type InvariantOracle struct {
	ID   string `json:"id"`
	Test string `json:"test"`
}

type GateBinding struct {
	ID    string     `json:"id"`
	Steps []GateStep `json:"steps"`
}

type GateStep struct {
	ID      string   `json:"id"`
	Kind    string   `json:"kind"`
	Argv    []string `json:"argv"`
	Oracles []string `json:"oracles"`
}

var (
	oracleIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,95}$`)
	stepIDPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)
	testNamePattern = regexp.MustCompile(`^Test[A-Za-z0-9_]+$`)
)

func LoadRegistry(root string) (Registry, error) {
	path := filepath.Join(root, filepath.FromSlash(RegistryPath))
	data, err := os.ReadFile(path)
	if err != nil {
		return Registry{}, fmt.Errorf("read R7 requirements registry: %w", err)
	}
	registry, err := DecodeRegistry(data)
	if err != nil {
		return Registry{}, fmt.Errorf("decode R7 requirements registry: %w", err)
	}
	canonical, err := canonicalJSON(registry)
	if err != nil {
		return Registry{}, err
	}
	if !bytes.Equal(data, canonical) {
		return Registry{}, fmt.Errorf("R7 requirements registry is not canonical JSON")
	}
	return registry, nil
}

func DecodeRegistry(data []byte) (Registry, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var registry Registry
	if err := decoder.Decode(&registry); err != nil {
		return Registry{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Registry{}, fmt.Errorf("multiple JSON values")
		}
		return Registry{}, err
	}
	return registry, nil
}

func ValidateBindings(root string, contract Contract, registry Registry) error {
	if registry.SchemaVersion != RegistrySchemaVersion {
		return fmt.Errorf("registry schema_version = %d, want %d",
			registry.SchemaVersion, RegistrySchemaVersion)
	}
	if registry.Invariants == nil || registry.Gates == nil {
		return fmt.Errorf("registry closed lists must be non-null")
	}
	if len(registry.Invariants) != InvariantCount || len(registry.Gates) != GateCount {
		return fmt.Errorf("registry has %d invariants and %d gates, want %d and %d",
			len(registry.Invariants), len(registry.Gates), InvariantCount, GateCount)
	}
	if err := validateInvariantBindings(root, contract, registry.Invariants); err != nil {
		return err
	}
	return validateGateBindings(contract, registry.Gates)
}

func validateInvariantBindings(root string, contract Contract, bindings []InvariantBinding) error {
	ids := make([]string, len(bindings))
	for index, binding := range bindings {
		ids[index] = binding.ID
		if binding.Oracles == nil || len(binding.Oracles) == 0 {
			return fmt.Errorf("invariant %s has no non-null oracle binding", binding.ID)
		}
		previous := ""
		for _, oracle := range binding.Oracles {
			if !oracleIDPattern.MatchString(oracle.ID) {
				return fmt.Errorf("invariant %s has invalid oracle ID %q", binding.ID, oracle.ID)
			}
			if previous != "" && oracle.ID <= previous {
				return fmt.Errorf("invariant %s oracles are not sorted and unique", binding.ID)
			}
			previous = oracle.ID
			if err := validateTestReference(root, oracle.Test); err != nil {
				return fmt.Errorf("invariant %s oracle %s: %w", binding.ID, oracle.ID, err)
			}
		}
	}
	if !slices.Equal(ids, contract.Invariants) {
		return fmt.Errorf("registry invariant IDs = %v, want exact contract list %v",
			ids, contract.Invariants)
	}
	return nil
}

func validateGateBindings(contract Contract, bindings []GateBinding) error {
	ids := make([]string, len(bindings))
	steps := make(map[string]GateStep)
	for index, binding := range bindings {
		ids[index] = binding.ID
		if binding.Steps == nil || len(binding.Steps) == 0 {
			return fmt.Errorf("gate %s has no non-null steps", binding.ID)
		}
		previous := ""
		for _, step := range binding.Steps {
			if !stepIDPattern.MatchString(step.ID) || (previous != "" && step.ID <= previous) {
				return fmt.Errorf("gate %s step IDs are invalid or not sorted and unique", binding.ID)
			}
			previous = step.ID
			if err := validateStep(step); err != nil {
				return fmt.Errorf("gate %s step %s: %w", binding.ID, step.ID, err)
			}
			if existing, found := steps[step.ID]; found && !stepsEqual(existing, step) {
				return fmt.Errorf("shared step %s differs between gates", step.ID)
			}
			steps[step.ID] = step
		}
	}
	if !slices.Equal(ids, contract.Gates) {
		return fmt.Errorf("registry gate IDs = %v, want exact contract list %v", ids, contract.Gates)
	}
	return nil
}

func validateStep(step GateStep) error {
	if step.Kind != "go-test" && step.Kind != "shell" {
		return fmt.Errorf("unknown kind %q", step.Kind)
	}
	if step.Argv == nil || len(step.Argv) == 0 || step.Oracles == nil || len(step.Oracles) == 0 {
		return fmt.Errorf("argv and oracles must be non-null and non-empty")
	}
	for _, argument := range step.Argv {
		if argument == "" || strings.ContainsRune(argument, '\x00') {
			return fmt.Errorf("argv contains an empty or NUL argument")
		}
	}
	previous := ""
	for _, oracle := range step.Oracles {
		if oracle == "" || strings.ContainsAny(oracle, "\r\n") ||
			(previous != "" && oracle <= previous) {
			return fmt.Errorf("oracles are invalid or not sorted and unique")
		}
		prefix := "stdout:"
		if step.Kind == "go-test" {
			prefix = "test:"
		}
		if !strings.HasPrefix(oracle, prefix) || len(oracle) == len(prefix) {
			return fmt.Errorf("oracle %q does not match %s step", oracle, step.Kind)
		}
		previous = oracle
	}
	if step.Kind == "go-test" && !slices.Contains(step.Argv, "-json") {
		return fmt.Errorf("go-test argv does not contain -json")
	}
	return nil
}

func stepsEqual(left, right GateStep) bool {
	return left.ID == right.ID && left.Kind == right.Kind &&
		slices.Equal(left.Argv, right.Argv) && slices.Equal(left.Oracles, right.Oracles)
}

func invariantTests(registry Registry) []string {
	set := make(map[string]struct{})
	for _, invariant := range registry.Invariants {
		for _, oracle := range invariant.Oracles {
			set[oracle.Test] = struct{}{}
		}
	}
	tests := make([]string, 0, len(set))
	for test := range set {
		tests = append(tests, test)
	}
	slices.Sort(tests)
	return tests
}

func uniqueSteps(registry Registry) []GateStep {
	byID := make(map[string]GateStep)
	for _, gate := range registry.Gates {
		for _, step := range gate.Steps {
			byID[step.ID] = step
		}
	}
	steps := make([]GateStep, 0, len(byID))
	for _, step := range byID {
		steps = append(steps, step)
	}
	slices.SortFunc(steps, func(left, right GateStep) int {
		return strings.Compare(left.ID, right.ID)
	})
	return steps
}

func validateTestReference(root, reference string) error {
	packagePath, symbol, ok := strings.Cut(reference, "::")
	if !ok || strings.Contains(symbol, "::") || !strings.HasPrefix(packagePath, "./") ||
		!testNamePattern.MatchString(symbol) {
		return fmt.Errorf("invalid test reference %q", reference)
	}
	directory := filepath.Join(root, "harness", filepath.FromSlash(strings.TrimPrefix(packagePath, "./")))
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("read test package %s: %w", packagePath, err)
	}
	found := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(directory, entry.Name()), nil,
			parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("parse %s/%s: %w", packagePath, entry.Name(), err)
		}
		for _, declaration := range file.Decls {
			function, isFunction := declaration.(*ast.FuncDecl)
			if !isFunction || function.Recv != nil || function.Name.Name != symbol {
				continue
			}
			if function.Body == nil || len(function.Body.List) == 0 {
				return fmt.Errorf("test %s exists but has an empty body", reference)
			}
			found++
		}
	}
	if found != 1 {
		return fmt.Errorf("test %s has %d declarations, want exactly one", reference, found)
	}
	return nil
}

func canonicalJSON(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal canonical JSON: %w", err)
	}
	return append(data, '\n'), nil
}
