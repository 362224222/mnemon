package main

import "testing"

func TestAllowedHarnessImportKeepsThinCommandAndBottomModel(t *testing.T) {
	for _, edge := range [][2]string{{"cmd", "node"}, {"cmd", "model"}, {"agent", "teamwork"}, {"store", "artifact"},
		{"authority", "agency"}, {"selector", "agency"}, {"node", "authority"},
		{"cas", "agency"}, {"peerlink", "agency"}, {"peerlink", "cas"}} {
		if !allowedHarnessImport(edge[0], edge[1]) {
			t.Errorf("expected allowed edge %s -> %s", edge[0], edge[1])
		}
	}
	for _, edge := range [][2]string{{"cmd", "agent"}, {"cmd", "assets"}, {"node", "localapi"}, {"agent", "localapi"},
		{"store", "teamwork"}, {"agency", "store"}, {"selector", "authority"}, {"authority", "store"},
		{"agency", "model"}, {"authority", "model"}, {"selector", "model"},
		{"agency", "selector"}, {"authority", "selector"}, {"node", "selector"}, {"peer", "selector"},
		{"localapi", "selector"}, {"agencycli", "selector"}, {"agencycli", "localapi"},
		{"agencycli", "model"}, {"agencycli", "node"}, {"cas", "authority"},
		{"peerlink", "authority"}, {"cas", "peerlink"}} {
		if allowedHarnessImport(edge[0], edge[1]) {
			t.Errorf("unexpected allowed edge %s -> %s", edge[0], edge[1])
		}
	}
}
