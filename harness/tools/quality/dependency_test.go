package main

import (
	"testing"
)

func TestAllowedHarnessImportKeepsThinCommandAndBottomModel(t *testing.T) {
	for _, edge := range [][2]string{{"cmd", "node"}, {"cmd", "model"}, {"agent", "teamwork"},
		{"store", "artifact"}} {
		if !allowedHarnessImport(edge[0], edge[1]) {
			t.Errorf("expected allowed edge %s -> %s", edge[0], edge[1])
		}
	}
	for _, edge := range [][2]string{{"cmd", "agent"}, {"cmd", "assets"}, {"node", "localapi"}, {"agent", "localapi"}, {"store", "teamwork"}} {
		if allowedHarnessImport(edge[0], edge[1]) {
			t.Errorf("unexpected allowed edge %s -> %s", edge[0], edge[1])
		}
	}
}

func TestAllowedHarnessPackageImportKeepsNodeControlDependencyOneWay(t *testing.T) {
	if !allowedHarnessPackageImport("harness/internal/localapi/nodecontrol", "harness/internal/localapi") {
		t.Fatal("nodecontrol child-to-parent adapter dependency was rejected")
	}
	for _, edge := range [][2]string{
		{"harness/internal/localapi", "harness/internal/localapi/nodecontrol"},
		{"harness/internal/localapi/nodecontrol", "harness/internal/localapi/other"},
		{"harness/internal/localapi/other", "harness/internal/localapi/nodecontrol"},
	} {
		if allowedHarnessPackageImport(edge[0], edge[1]) {
			t.Errorf("unexpected allowed package edge %s -> %s", edge[0], edge[1])
		}
	}
}

func TestDependencyFindingsKeepNodeControlParentEdgeOneWay(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "harness/internal/localapi/nodecontrol/good.go", `package nodecontrol
import _ "github.com/mnemon-dev/mnemon/harness/internal/localapi"
`)
	writeTestFile(t, root, "harness/internal/localapi/bad.go", `package localapi
import _ "github.com/mnemon-dev/mnemon/harness/internal/localapi/nodecontrol"
`)
	writeTestFile(t, root, "harness/internal/localapi/other/bad.go", `package other
import _ "github.com/mnemon-dev/mnemon/harness/internal/localapi/nodecontrol"
`)
	findings, err := dependencyFindings(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"dependency_direction:harness/internal/localapi/other::harness/internal/localapi/nodecontrol",
		"dependency_direction:harness/internal/localapi::harness/internal/localapi/nodecontrol",
	}
	if len(findings) != len(want) {
		t.Fatalf("package direction findings = %#v", findings)
	}
	for index := range want {
		if findings[index].Identity != want[index] {
			t.Fatalf("finding %d = %#v, want %s", index, findings[index], want[index])
		}
	}
}
