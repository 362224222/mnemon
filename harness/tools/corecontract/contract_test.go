package corecontract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTrackedR7ContractHasClosedLifecycleAndIDs(t *testing.T) {
	root := filepath.Clean("../../..")
	contract, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if contract.Lifecycle != LifecycleActive || len(contract.Invariants) != InvariantCount ||
		len(contract.Gates) != GateCount {
		t.Fatalf("contract = lifecycle %s invariants %v gates %v",
			contract.Lifecycle, contract.Invariants, contract.Gates)
	}
}

func TestContractParserAcceptsOnlyOneKnownLifecycleAndClosedLists(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("../../..", filepath.FromSlash(DocumentPath)))
	if err != nil {
		t.Fatal(err)
	}
	for _, lifecycle := range []Lifecycle{LifecycleProposed, LifecycleActive, LifecycleRetired} {
		changed := strings.Replace(string(data), "Status: **ACTIVE**.",
			"Status: **"+string(lifecycle)+"**.", 1)
		contract, err := Parse([]byte(changed))
		if err != nil || contract.Lifecycle != lifecycle {
			t.Fatalf("parse %s = %+v, %v", lifecycle, contract, err)
		}
	}
	duplicate := strings.Replace(string(data), "**P-01 Admission owns facts.**",
		"**P-01 Admission owns facts.**\n**P-01 Duplicate.**", 1)
	if _, err := Parse([]byte(duplicate)); err == nil || !strings.Contains(err.Error(), "repeats invariant") {
		t.Fatalf("duplicate invariant error = %v", err)
	}
	missing := strings.Replace(string(data), "Status: **ACTIVE**.", "Status: active.", 1)
	if _, err := Parse([]byte(missing)); err == nil || !strings.Contains(err.Error(), "no canonical") {
		t.Fatalf("missing status error = %v", err)
	}
}
