package main

import (
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
)

const (
	manifestSchemaVersion = 1
	qualityToolVersion    = "harness-quality/v1"
	duplicateTokenMinimum = 150
)

const (
	ruleCyclomatic     = "cyclomatic_complexity"
	ruleCognitive      = "cognitive_complexity"
	ruleFunctionLines  = "function_logical_lines"
	ruleStatements     = "function_statements"
	ruleNesting        = "control_flow_nesting"
	ruleProductionFile = "production_file_lines"
	rulePairedTestFile = "paired_test_file_lines"
	ruleDuplicate      = "normalized_duplicate_tokens"
)

type threshold struct {
	Rule  string `json:"rule"`
	Limit int    `json:"limit"`
}

var qualityThresholds = []threshold{
	{Rule: ruleCognitive, Limit: 25},
	{Rule: ruleNesting, Limit: 4},
	{Rule: ruleCyclomatic, Limit: 20},
	{Rule: ruleFunctionLines, Limit: 80},
	{Rule: ruleStatements, Limit: 50},
	{Rule: ruleDuplicate, Limit: duplicateTokenMinimum - 1},
	{Rule: rulePairedTestFile, Limit: 800},
	{Rule: ruleProductionFile, Limit: 400},
}

type baselineManifest struct {
	SchemaVersion int             `json:"schema_version"`
	ToolVersion   string          `json:"tool_version"`
	SourceCommit  string          `json:"source_commit"`
	Thresholds    []threshold     `json:"thresholds"`
	Entries       []baselineEntry `json:"entries"`
}

type baselineEntry struct {
	Rule        string   `json:"rule"`
	Identity    string   `json:"identity"`
	Path        string   `json:"path"`
	Symbol      string   `json:"symbol,omitempty"`
	DebtID      string   `json:"debt_id,omitempty"`
	Owners      []string `json:"owners,omitempty"`
	Fingerprint string   `json:"fingerprint,omitempty"`
	Ceiling     int      `json:"ceiling"`
}

type exceptionManifest struct {
	SchemaVersion int              `json:"schema_version"`
	Entries       []exceptionEntry `json:"entries"`
}

type exceptionEntry struct {
	Rule              string `json:"rule"`
	Identity          string `json:"identity"`
	Path              string `json:"path"`
	Symbol            string `json:"symbol,omitempty"`
	Component         string `json:"component,omitempty"`
	Ceiling           int    `json:"ceiling"`
	Reason            string `json:"reason"`
	Risk              string `json:"risk"`
	Owner             string `json:"owner"`
	RemovalCheckpoint string `json:"removal_checkpoint"`
}

type architectureManifest struct {
	SchemaVersion int                 `json:"schema_version"`
	SourceCommit  string              `json:"source_commit"`
	Entries       []architectureEntry `json:"entries"`
}

type architectureEntry struct {
	Rule              string `json:"rule"`
	Identity          string `json:"identity"`
	Path              string `json:"path"`
	Symbol            string `json:"symbol,omitempty"`
	Component         string `json:"component,omitempty"`
	Risk              string `json:"risk"`
	Evidence          string `json:"evidence"`
	Owner             string `json:"owner"`
	RemovalCheckpoint string `json:"removal_checkpoint"`
}

type requirementsManifest = corecontract.Registry
type requirementRecord = corecontract.EvidenceRecord

func functionIdentity(rule, path, symbol string) string {
	return fmt.Sprintf("%s:%s::%s", rule, path, symbol)
}

func fileIdentity(rule, path string) string {
	return rule + ":" + path
}
