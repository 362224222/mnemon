package main

import "testing"

func TestAllowedHarnessImportKeepsThinCommandAndBottomModel(t *testing.T) {
	for _, edge := range [][2]string{{"cmd", "node"}, {"cmd", "model"}, {"agent", "teamwork"}, {"store", "artifact"}} {
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
