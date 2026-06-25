package main

import (
	"path/filepath"
	"testing"
)

func TestProdSimStrictTopologyRequiresDistinctMnemondPerAgent(t *testing.T) {
	agents := []r1CodexSyncAgent{
		{r1CodexAgent: r1CodexAgent{principal: "codex-01@project", workspace: filepath.Join("run", "workspaces", "codex-01")}},
		{r1CodexAgent: r1CodexAgent{principal: "codex-02@project", workspace: filepath.Join("run", "workspaces", "codex-02")}},
		{r1CodexAgent: r1CodexAgent{principal: "codex-03@project", workspace: filepath.Join("run", "workspaces", "codex-03")}},
		{r1CodexAgent: r1CodexAgent{principal: "codex-04@project", workspace: filepath.Join("run", "workspaces", "codex-04")}},
		{r1CodexAgent: r1CodexAgent{principal: "codex-05@project", workspace: filepath.Join("run", "workspaces", "codex-05")}},
	}
	top := buildR1ProdSimTopology(agents)
	if !prodSimStrictTopology(top) {
		t.Fatalf("strict topology rejected distinct per-agent stores: %+v", top)
	}
	top.AgentMnemondMap["codex-05@project"] = top.AgentMnemondMap["codex-04@project"]
	if prodSimStrictTopology(top) {
		t.Fatalf("strict topology accepted duplicate mnemond path: %+v", top)
	}
	top = buildR1ProdSimTopology(agents)
	top.SharedMnemond = true
	if prodSimStrictTopology(top) {
		t.Fatalf("strict topology accepted shared mnemond flag: %+v", top)
	}
}

func TestAllProdSimScenariosOKRequiresEveryScenario(t *testing.T) {
	all := []r1TaskSimScenarioReport{
		{Name: "bootstrap_profiles", Status: "ok"},
		{Name: "split_work", Status: "ok"},
		{Name: "dependency_handoff", Status: "ok"},
		{Name: "blocker_rework", Status: "ok"},
		{Name: "ttl_paused_agent", Status: "ok"},
		{Name: "duplicate_pull_restart", Status: "ok"},
	}
	if !allProdSimScenariosOK(all) {
		t.Fatalf("all scenarios should pass")
	}
	missing := all[:len(all)-1]
	if allProdSimScenariosOK(missing) {
		t.Fatalf("missing scenario should fail")
	}
}
