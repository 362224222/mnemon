package main

import "testing"

func TestAllowedHarnessImportMatchesR7ModuleLayout(t *testing.T) {
	for _, edge := range [][2]string{
		{"authority", "agency"}, {"selector", "agency"}, {"cli", "agency"},
		{"cas", "agency"}, {"peerlink", "agency"}, {"peerlink", "cas"},
		{"attach", "agency"}, {"daemon", "agency"}, {"daemon", "authority"},
		{"daemon", "cas"}, {"daemon", "peerlink"}, {"cmd", "attach"}, {"cmd", "cli"},
		{"cmd", "daemon"},
	} {
		if !allowedHarnessImport(edge[0], edge[1]) {
			t.Errorf("expected allowed edge %s -> %s", edge[0], edge[1])
		}
	}
	for _, edge := range [][2]string{
		{"agency", "authority"}, {"selector", "authority"}, {"authority", "cas"},
		{"agency", "selector"}, {"cli", "selector"}, {"cli", "daemon"},
		{"cas", "authority"},
		{"peerlink", "authority"}, {"cas", "peerlink"}, {"attach", "authority"},
		{"attach", "cas"}, {"attach", "peerlink"}, {"daemon", "cli"},
		{"cmd", "agency"}, {"cmd", "authority"}, {"cmd", "cas"}, {"cmd", "peerlink"},
		{"cmd", "selector"},
	} {
		if allowedHarnessImport(edge[0], edge[1]) {
			t.Errorf("unexpected allowed edge %s -> %s", edge[0], edge[1])
		}
	}
}
