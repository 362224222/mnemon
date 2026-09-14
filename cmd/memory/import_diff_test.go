package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/internal/memory/importdraft"
	"github.com/mnemon-dev/mnemon/internal/memory/model"
	"github.com/mnemon-dev/mnemon/internal/memory/store"
)

func configureImportDiffTest(t *testing.T) {
	t.Helper()
	configureRememberDiffTest(t)
	oldNoDiff, oldDryRun := importNoDiff, importDryRun
	t.Cleanup(func() { importNoDiff, importDryRun = oldNoDiff, oldDryRun })
	importNoDiff, importDryRun = false, false
}

type importDiffOutput struct {
	Imported int `json:"imported"`
	Updated  int `json:"updated"`
	Skipped  int `json:"skipped"`
	Errors   int `json:"errors"`
	Results  []struct {
		Index   int    `json:"index"`
		ID      string `json:"id"`
		Content string `json:"content"`
		Action  string `json:"action"`
	} `json:"results"`
}

func importForDiffTest(t *testing.T, contents []string, edges []importdraft.DraftEdge) importDiffOutput {
	t.Helper()
	draft := importdraft.MemoryDraft{SchemaVersion: "1", Edges: edges}
	for _, content := range contents {
		draft.Insights = append(draft.Insights, importdraft.DraftInsight{
			Content: content, Category: "fact", Importance: 5,
		})
	}
	data, err := json.Marshal(draft)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "draft.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var runErr error
	out := captureStdout(t, func() { runErr = importCmd.RunE(importCmd, []string{path}) })
	if runErr != nil {
		t.Fatalf("import: %v", runErr)
	}
	var result importDiffOutput
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("decode import output: %v\n%s", err, out)
	}
	if result.Errors != 0 || len(result.Results) != len(contents) {
		t.Fatalf("incomplete import: %+v", result)
	}
	return result
}

func TestImportPreservesDistinctContent(t *testing.T) {
	const alpha = "Project Alpha uses PostgreSQL database for persistent application storage"
	const details = " with indexed customer records, transaction history, audit events, replication, backups, failover, monitoring, access controls, migrations, connection pooling, and disaster recovery"
	tests := []struct{ name, first, second string }{
		{"different subject", alpha, "Project Beta uses PostgreSQL database for persistent application storage"},
		{"changed value", alpha, "Project Alpha uses SQLite database for persistent application storage"},
		{"near duplicate", alpha + details, "Project Beta uses PostgreSQL database for persistent application storage" + details},
		{"conflict", alpha, "Project Alpha no longer uses PostgreSQL database for persistent application storage"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configureImportDiffTest(t)
			result := importForDiffTest(t, []string{tt.first, tt.second}, nil)
			if result.Imported != 2 || result.Updated != 0 || result.Skipped != 0 {
				t.Errorf("import = %+v, want two added insights", result)
			}
			assertActiveRememberContents(t, map[string]string{
				result.Results[0].ID: tt.first, result.Results[1].ID: tt.second,
			})
		})
	}
}

func TestImportPreservesExistingFact(t *testing.T) {
	configureImportDiffTest(t)
	const alpha = "Project Alpha uses PostgreSQL database for persistent application storage"
	const beta = "Project Beta uses PostgreSQL database for persistent application storage"
	first := rememberForDiffTest(t, alpha)
	result := importForDiffTest(t, []string{beta}, nil)
	if result.Imported != 1 || result.Updated != 0 || result.Skipped != 0 {
		t.Errorf("import = %+v, want one added insight", result)
	}
	assertActiveRememberContents(t, map[string]string{first.ID: alpha, result.Results[0].ID: beta})
}

func TestImportExactDuplicateRetainsIndexAndEdgeMapping(t *testing.T) {
	configureImportDiffTest(t)
	const content = "Project Alpha uses PostgreSQL database for persistent application storage"
	remImportance = 1
	first := rememberForDiffTest(t, content)
	want := map[string]string{first.ID: content}
	remImportance, remNoDiff = 5, true
	for _, detail := range []string{"backups", "replication", "indexes", "migrations", "monitoring", "transactions"} {
		extended := content + " with " + detail
		result := rememberForDiffTest(t, extended)
		want[result.ID] = extended
	}
	const linked = "Zebra migration survey notes"
	result := importForDiffTest(t, []string{content, linked, linked}, []importdraft.DraftEdge{
		{SourceIndex: 0, TargetIndex: 2, EdgeType: "semantic", Weight: 0.75, Reason: "exact duplicate index mapping"},
	})
	if result.Imported != 1 || result.Updated != 0 || result.Skipped != 2 {
		t.Errorf("import = %+v, want one added insight and two exact duplicates", result)
	}
	if result.Results[0].ID != first.ID || result.Results[0].Action != "skipped" {
		t.Errorf("existing duplicate = %+v, want skipped %s", result.Results[0], first.ID)
	}
	linkedID := result.Results[1].ID
	if result.Results[2].ID != linkedID || result.Results[2].Action != "skipped" {
		t.Errorf("batch duplicate = %+v, want skipped %s", result.Results[2], linkedID)
	}
	want[linkedID] = linked
	assertActiveRememberContents(t, want)
	db, err := store.Open(store.StoreDir(dataDir, store.DefaultStoreName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	edges, err := db.GetEdgesBySourceAndType(first.ID, model.EdgeSemantic)
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range edges {
		if edge.TargetID == linkedID && edge.Metadata["reason"] == "exact duplicate index mapping" {
			return
		}
	}
	t.Fatalf("explicit edge from existing duplicate %s to batch duplicate %s is missing", first.ID, linkedID)
}

func TestImportNoDiffStoresExactRepeats(t *testing.T) {
	configureImportDiffTest(t)
	importNoDiff = true
	const content = "Project Alpha uses PostgreSQL database for persistent application storage"
	result := importForDiffTest(t, []string{content, content}, nil)
	if result.Imported != 2 || result.Updated != 0 || result.Skipped != 0 {
		t.Errorf("import with --no-diff = %+v, want two added insights", result)
	}
	assertActiveRememberContents(t, map[string]string{
		result.Results[0].ID: content, result.Results[1].ID: content,
	})
}
