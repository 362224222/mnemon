package corecontract

import "fmt"

func expectedStepArgv(rule stepRule, report GateReport) ([]string, error) {
	runtimeName := ""
	switch rule.id {
	case "docker", "evidence-hermetic":
		runtimeName = "scripted"
	case "live", "evidence-live":
		runtimeName = "codex"
	default:
		return append([]string(nil), rule.argv...), nil
	}
	for _, bundle := range report.Bundles {
		if bundle.Runtime == runtimeName {
			return gateStepArgv(rule, bundle.RunID), nil
		}
	}
	return nil, fmt.Errorf("%s bundle is required by %s", runtimeName, rule.id)
}

func gateStepArgv(rule stepRule, runID string) []string {
	if rule.kind == "evidence" {
		return []string{"harness/test/e2e/runner/validate_evidence.sh", "--run", runID}
	}
	argv := append([]string(nil), rule.argv...)
	return append(argv, "--run", runID)
}
