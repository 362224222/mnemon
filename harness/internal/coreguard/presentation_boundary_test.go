package coreguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var presentationSemanticFiles = map[string]bool{
	"bootstrap_presenter.go":        true,
	"context_presenter.go":          true,
	"payload_contract_presenter.go": true,
	"teamwork_presenter.go":         true,
}

var teamworkPresentationTerms = []string{
	"agent_profile",
	"teamwork_signal",
	"assignment_ref",
	"progress_digest",
	"DerivedEventAssignment",
	"DerivedEventTeamwork",
}

func TestPresentationCoreDoesNotOwnTeamworkSemantics(t *testing.T) {
	root := filepath.Join("..", "mnemond", "presentation")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read presentation package: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if presentationSemanticFiles[name] {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		body := string(raw)
		for _, term := range teamworkPresentationTerms {
			if strings.Contains(body, term) {
				t.Errorf("presentation core file %s contains teamwork semantic term %q; keep dataflow semantics in presenter modules", name, term)
			}
		}
	}
}
